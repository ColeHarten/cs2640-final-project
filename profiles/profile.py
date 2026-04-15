"""CloudLab profile for a single-node multi-filesystem AsyncMux/Mux testbed.

This profile creates ONE RawPC node and configures multiple mounted tiers
on that node.

Supported tier styles:
- tmpfs-backed tier
- loopback-image-backed ext4 tier
- loopback-image-backed xfs tier

Default layout:
- /tier0/tmpfs   -> tmpfs (optional)
- /tier1/data    -> ext4 image-backed mount
- /tier2/data    -> xfs image-backed mount
- /tier3/data    -> ext4 image-backed mount (optional)

This is intended for:
- single-host AsyncMux correctness testing
- multi-filesystem placement/migration/promotion testing
- development before moving to multi-node remote tiers
"""

import geni.portal as portal
import geni.rspec.pg as pg

pc = portal.Context()
request = pc.makeRequestRSpec()

#
# Parameters
#

pc.defineParameter(
    "hardware_type",
    "Preferred hardware type (leave blank for any available raw PC)",
    portal.ParameterType.STRING,
    ""
)

pc.defineParameter(
    "disk_image",
    "Disk image",
    portal.ParameterType.STRING,
    "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
)

pc.defineParameter(
    "num_tiers",
    "Number of tiers to configure",
    portal.ParameterType.INTEGER,
    3,
    legalValues=[2, 3, 4]
)

pc.defineParameter(
    "install_deps",
    "Install common packages at boot",
    portal.ParameterType.BOOLEAN,
    True
)

pc.defineParameter(
    "repo_url",
    "Optional git repo URL to clone",
    portal.ParameterType.STRING,
    "https://github.com/ColeHarten/cs2640-final-project"
)

pc.defineParameter(
    "repo_branch",
    "Optional git branch",
    portal.ParameterType.STRING,
    "main"
)

pc.defineParameter(
    "workspace_dir",
    "Directory to store loopback filesystem image files",
    portal.ParameterType.STRING,
    "/local"
)

pc.defineParameter(
    "tier0_tmpfs_gb",
    "Tier0 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    4
)

pc.defineParameter(
    "tier1_size_gb",
    "Tier1 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    20
)

pc.defineParameter(
    "tier2_size_gb",
    "Tier2 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    50
)

pc.defineParameter(
    "tier3_size_gb",
    "Tier3 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    100
)

pc.defineParameter(
    "tier1_fs",
    "Tier1 filesystem type",
    portal.ParameterType.STRING,
    "ext4",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")]
)

pc.defineParameter(
    "tier2_fs",
    "Tier2 filesystem type",
    portal.ParameterType.STRING,
    "xfs",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")]
)

pc.defineParameter(
    "tier3_fs",
    "Tier3 filesystem type",
    portal.ParameterType.STRING,
    "ext4",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")]
)

params = pc.bindParameters()

#
# Helpers
#

def apply_node_defaults(node):
    if params.hardware_type.strip():
        node.hardware_type = params.hardware_type.strip()
    node.disk_image = params.disk_image

def combined_boot_script():
    repo_logic = r'''
if [ -n "{repo_url}" ]; then
  cd /users/$USER
  if [ ! -d AsyncMux ]; then
    git clone -b "{repo_branch}" "{repo_url}" AsyncMux
  fi
fi
'''.format(
        repo_url=params.repo_url.replace('"', '\\"'),
        repo_branch=params.repo_branch.replace('"', '\\"')
    )

    return r'''#!/usr/bin/env bash
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

if [ "{install_deps}" = "True" ]; then
  sudo apt-get update
  sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    git \
    clang \
    gdb \
    make \
    fio \
    jq \
    python3 \
    python3-pip \
    fuse3 \
    libfuse3-dev \
    xfsprogs \
    e2fsprogs \
    rsync \
    net-tools \
    iperf3 \
    util-linux
fi

command -v mount >/dev/null 2>&1 || {{ echo "mount missing"; exit 1; }}
command -v findmnt >/dev/null 2>&1 || {{ echo "findmnt missing"; exit 1; }}
command -v blkid >/dev/null 2>&1 || {{ echo "blkid missing"; exit 1; }}
command -v mkfs.ext4 >/dev/null 2>&1 || {{ echo "mkfs.ext4 missing"; exit 1; }}
command -v mkfs.xfs >/dev/null 2>&1 || {{ echo "mkfs.xfs missing"; exit 1; }}
command -v losetup >/dev/null 2>&1 || {{ echo "losetup missing"; exit 1; }}

mkdir -p /users/$USER/results
mkdir -p /users/$USER/bin
mkdir -p /users/$USER/mux-config
mkdir -p /users/$USER/mux-scripts
mkdir -p /users/$USER/workloads

{repo_logic}

WORKSPACE_DIR="{workspace_dir}"
NUM_TIERS="{num_tiers}"

TIER0_TMPFS_GB="{tier0_tmpfs_gb}"

TIER1_SIZE_GB="{tier1_size_gb}"
TIER2_SIZE_GB="{tier2_size_gb}"
TIER3_SIZE_GB="{tier3_size_gb}"

TIER1_FS="{tier1_fs}"
TIER2_FS="{tier2_fs}"
TIER3_FS="{tier3_fs}"

sudo mkdir -p "$WORKSPACE_DIR"

remove_fstab_entry() {{
  local mountpoint="$1"
  sudo sed -i "\|[[:space:]]$mountpoint[[:space:]]|d" /etc/fstab
}}

wait_for_mountpoint() {{
  local mountpoint="$1"
  local tries=10
  local i=0
  while [ "$i" -lt "$tries" ]; do
    if mountpoint -q "$mountpoint"; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  echo "ERROR: timed out waiting for mountpoint $mountpoint"
  return 1
}}

setup_tmpfs_tier() {{
  local mountpoint="$1"
  local size_gb="$2"

  sudo mkdir -p "$mountpoint"

  if mountpoint -q "$mountpoint"; then
    sudo umount "$mountpoint" || true
  fi

  remove_fstab_entry "$mountpoint"

  if [ "$size_gb" -gt 0 ]; then
    sudo mount -t tmpfs -o size="${{size_gb}}G" tmpfs "$mountpoint"
    wait_for_mountpoint "$mountpoint"
    echo "tmpfs $mountpoint tmpfs rw,nosuid,nodev,size=${{size_gb}}G 0 0" | sudo tee -a /etc/fstab >/dev/null
    findmnt "$mountpoint"
  fi
}}

setup_loop_fs_tier() {{
  local mount_base="$1"
  local image_path="$2"
  local size_gb="$3"
  local fs_type="$4"

  local data_mount="${{mount_base}}/data"
  local loopdev=""
  local current_type=""

  sudo mkdir -p "$mount_base"
  sudo mkdir -p "$data_mount"
  sudo mkdir -p "${{mount_base}}/logs"
  sudo mkdir -p "${{mount_base}}/cache"
  sudo mkdir -p "${{mount_base}}/meta"

  if mountpoint -q "$data_mount"; then
    sudo umount "$data_mount" || true
  fi

  remove_fstab_entry "$data_mount"

  sudo mkdir -p "$(dirname "$image_path")"
  sudo truncate -s "${{size_gb}}G" "$image_path"

  loopdev="$(sudo losetup --find --show "$image_path")"

  if sudo blkid -o value -s TYPE "$loopdev" >/dev/null 2>&1; then
    current_type="$(sudo blkid -o value -s TYPE "$loopdev")"
  fi

  if [ "$current_type" != "$fs_type" ]; then
    echo "Formatting $loopdev as $fs_type (was: ${{current_type:-none}})"
    if [ "$fs_type" = "ext4" ]; then
      sudo mkfs.ext4 -F "$loopdev"
    elif [ "$fs_type" = "xfs" ]; then
      sudo mkfs.xfs -f "$loopdev"
    else
      echo "Unsupported filesystem type: $fs_type"
      sudo losetup -d "$loopdev" || true
      exit 1
    fi
  fi

  sudo mount -t "$fs_type" "$loopdev" "$data_mount"
  wait_for_mountpoint "$data_mount"

  local detected_type
  detected_type="$(findmnt -n -o FSTYPE --target "$data_mount")"
  if [ "$detected_type" != "$fs_type" ]; then
    echo "ERROR: mounted $data_mount as $detected_type but expected $fs_type"
    sudo umount "$data_mount" || true
    sudo losetup -d "$loopdev" || true
    exit 1
  fi

  echo "$image_path $data_mount $fs_type loop,rw,nosuid,nodev 0 0" | sudo tee -a /etc/fstab >/dev/null

  echo "=== mounted $data_mount ==="
  findmnt "$data_mount" || true
  mount | grep "$data_mount" || true
  stat -f -c '%n %T %t' "$data_mount" || true
  sudo blkid "$image_path" || true

  sudo losetup -d "$loopdev"
}}

write_tier_conf() {{
  local tier_idx="$1"
  local mount_base="$2"
  local data_mount="$3"
  local fs_type="$4"
  local image_path="$5"
  local tmpfs_mount="$6"

  cat <<EOF | sudo tee "${{mount_base}}/tier.conf" >/dev/null
tier_index=${{tier_idx}}
data_mount=${{data_mount}}
fs_type=${{fs_type}}
image_path=${{image_path}}
tmpfs_mount=${{tmpfs_mount}}
EOF
}}

sudo mkdir -p /tier0 /tier1 /tier2 /tier3
sudo mkdir -p /tier0/logs /tier0/cache /tier0/meta
sudo mkdir -p /tier1/logs /tier1/cache /tier1/meta
sudo mkdir -p /tier2/logs /tier2/cache /tier2/meta
sudo mkdir -p /tier3/logs /tier3/cache /tier3/meta

if [ "$TIER0_TMPFS_GB" -gt 0 ]; then
  setup_tmpfs_tier /tier0/tmpfs "$TIER0_TMPFS_GB"
  write_tier_conf 0 /tier0 "" "tmpfs" "" "/tier0/tmpfs"
else
  write_tier_conf 0 /tier0 "" "disabled" "" ""
fi

if [ "$NUM_TIERS" -ge 2 ]; then
  setup_loop_fs_tier /tier1 "$WORKSPACE_DIR/tier1.img" "$TIER1_SIZE_GB" "$TIER1_FS"
  write_tier_conf 1 /tier1 /tier1/data "$TIER1_FS" "$WORKSPACE_DIR/tier1.img" ""
fi

if [ "$NUM_TIERS" -ge 3 ]; then
  setup_loop_fs_tier /tier2 "$WORKSPACE_DIR/tier2.img" "$TIER2_SIZE_GB" "$TIER2_FS"
  write_tier_conf 2 /tier2 /tier2/data "$TIER2_FS" "$WORKSPACE_DIR/tier2.img" ""
fi

if [ "$NUM_TIERS" -ge 4 ]; then
  setup_loop_fs_tier /tier3 "$WORKSPACE_DIR/tier3.img" "$TIER3_SIZE_GB" "$TIER3_FS"
  write_tier_conf 3 /tier3 /tier3/data "$TIER3_FS" "$WORKSPACE_DIR/tier3.img" ""
fi

cat > /users/$USER/mux-config/topology.env <<EOF
NUM_TIERS=$NUM_TIERS
TIER0_TMPFS=/tier0/tmpfs
TIER1_DATA=/tier1/data
TIER2_DATA=/tier2/data
TIER3_DATA=/tier3/data
TIER1_FS=$TIER1_FS
TIER2_FS=$TIER2_FS
TIER3_FS=$TIER3_FS
WORKSPACE_DIR=$WORKSPACE_DIR
EOF

cat > /users/$USER/mux-scripts/show-topology.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "Node: $(hostname)"
echo
echo "Mounted tiers:"
mount | grep '/tier' || true
echo
echo "findmnt:"
findmnt /tier0/tmpfs || true
findmnt /tier1/data || true
findmnt /tier2/data || true
findmnt /tier3/data || true
echo
echo "Tier configs:"
find /tier0 /tier1 /tier2 /tier3 -name tier.conf 2>/dev/null -print -exec cat {{}} \;
EOF
chmod +x /users/$USER/mux-scripts/show-topology.sh

cat > /users/$USER/mux-scripts/debug-mounts.sh <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "=== mount ==="
mount | grep /tier || true
echo
echo "=== findmnt ==="
findmnt /tier0/tmpfs || true
findmnt /tier1/data || true
findmnt /tier2/data || true
findmnt /tier3/data || true
echo
echo "=== statfs ==="
stat -f -c '%n %T %t' /tier0/tmpfs || true
stat -f -c '%n %T %t' /tier1/data || true
stat -f -c '%n %T %t' /tier2/data || true
stat -f -c '%n %T %t' /tier3/data || true
echo
echo "=== blkid ==="
sudo blkid "$WORKSPACE_DIR/tier1.img" || true
sudo blkid "$WORKSPACE_DIR/tier2.img" || true
sudo blkid "$WORKSPACE_DIR/tier3.img" || true
EOF
chmod +x /users/$USER/mux-scripts/debug-mounts.sh

cat > /users/$USER/workloads/smoke.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "Single-node smoke test from $(hostname)"
cmake --version
fio --version || true
mount | grep '/tier' || true
findmnt /tier0/tmpfs || true
findmnt /tier1/data || true
findmnt /tier2/data || true
findmnt /tier3/data || true
EOF
chmod +x /users/$USER/workloads/smoke.sh

cat > /users/$USER/README-CLOUDLAB-TOPOLOGY.txt <<EOF
Single-node CloudLab multitier testbed

Expected mounts:
- /tier0/tmpfs  : optional tmpfs tier
- /tier1/data   : image-backed filesystem ($TIER1_FS)
- /tier2/data   : image-backed filesystem ($TIER2_FS)
- /tier3/data   : image-backed filesystem ($TIER3_FS, if enabled)

This profile is intended to support:
- AsyncMux single-host development
- multi-filesystem correctness tests
- tier placement/migration/promotion on one node
EOF

echo "Single-node multi-filesystem setup complete on $(hostname)"
echo "==== FINAL TIER STATUS ===="
mount | grep '/tier' || true
findmnt /tier0/tmpfs || true
findmnt /tier1/data || true
findmnt /tier2/data || true
findmnt /tier3/data || true
stat -f -c '%n %T %t' /tier0/tmpfs || true
stat -f -c '%n %T %t' /tier1/data || true
stat -f -c '%n %T %t' /tier2/data || true
stat -f -c '%n %T %t' /tier3/data || true
'''.format(
        install_deps=params.install_deps,
        repo_logic=repo_logic,
        workspace_dir=params.workspace_dir.replace('"', '\\"'),
        num_tiers=params.num_tiers,
        tier0_tmpfs_gb=params.tier0_tmpfs_gb,
        tier1_size_gb=params.tier1_size_gb,
        tier2_size_gb=params.tier2_size_gb,
        tier3_size_gb=params.tier3_size_gb,
        tier1_fs=params.tier1_fs,
        tier2_fs=params.tier2_fs,
        tier3_fs=params.tier3_fs,
    )

def add_common_services(node):
    node.addService(pg.Execute(shell="bash", command=combined_boot_script()))

node = request.RawPC("muxnode")
apply_node_defaults(node)
add_common_services(node)

pc.printRequestRSpec(request)