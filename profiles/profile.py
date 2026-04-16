#!/usr/bin/env python3

"""
CloudLab Single-Node Multi-Filesystem AsyncMux Testbed
"""

import base64
import geni.portal as portal
import geni.rspec.pg as pg

pc = portal.Context()
request = pc.makeRequestRSpec()

pc.defineParameter(
    "hardware_type",
    "Preferred hardware type (leave blank for any available raw PC)",
    portal.ParameterType.STRING,
    "",
)
pc.defineParameter(
    "disk_image",
    "Disk image",
    portal.ParameterType.STRING,
    "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD",
)
pc.defineParameter(
    "cloudlab_user",
    "CloudLab username (optional; leave blank to auto-detect under /users)",
    portal.ParameterType.STRING,
    "",
)
pc.defineParameter(
    "num_tiers",
    "Number of tiers to configure",
    portal.ParameterType.INTEGER,
    3,
    legalValues=[2, 3, 4],
)
pc.defineParameter(
    "install_deps",
    "Install common packages at boot",
    portal.ParameterType.BOOLEAN,
    True,
)
pc.defineParameter(
    "repo_url",
    "Optional git repo URL to clone",
    portal.ParameterType.STRING,
    "https://github.com/ColeHarten/cs2640-final-project",
)
pc.defineParameter(
    "repo_branch",
    "Optional git branch",
    portal.ParameterType.STRING,
    "main",
)
pc.defineParameter(
    "workspace_dir",
    "Directory to store loopback filesystem image files",
    portal.ParameterType.STRING,
    "/local",
)
pc.defineParameter(
    "tier0_tmpfs_gb",
    "Tier0 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    4,
)
pc.defineParameter(
    "tier1_size_gb",
    "Tier1 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    20,
)
pc.defineParameter(
    "tier2_size_gb",
    "Tier2 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    50,
)
pc.defineParameter(
    "tier3_size_gb",
    "Tier3 image-backed filesystem size in GB",
    portal.ParameterType.INTEGER,
    100,
)
pc.defineParameter(
    "tier1_fs",
    "Tier1 filesystem type",
    portal.ParameterType.STRING,
    "ext4",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")],
)
pc.defineParameter(
    "tier2_fs",
    "Tier2 filesystem type",
    portal.ParameterType.STRING,
    "xfs",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")],
)
pc.defineParameter(
    "tier3_fs",
    "Tier3 filesystem type",
    portal.ParameterType.STRING,
    "ext4",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")],
)

params = pc.bindParameters()


def sq(s):
    return s.replace("'", "'\"'\"'")


def apply_node_defaults(node):
    if params.hardware_type.strip():
        node.hardware_type = params.hardware_type.strip()
    node.disk_image = params.disk_image


def combined_boot_script():
    install_deps_val = str(params.install_deps)
    repo_url_val = sq(params.repo_url)
    repo_branch_val = sq(params.repo_branch)
    workspace_dir_val = sq(params.workspace_dir)
    cloudlab_user_val = sq(params.cloudlab_user)
    num_tiers_val = str(params.num_tiers)
    tier0_tmpfs_gb_val = str(params.tier0_tmpfs_gb)
    tier1_size_gb_val = str(params.tier1_size_gb)
    tier2_size_gb_val = str(params.tier2_size_gb)
    tier3_size_gb_val = str(params.tier3_size_gb)
    tier1_fs_val = params.tier1_fs
    tier2_fs_val = params.tier2_fs
    tier3_fs_val = params.tier3_fs

    script = "#!/usr/bin/env bash\n"
    script += "set -euxo pipefail\n"
    script += "\n"
    script += "export DEBIAN_FRONTEND=noninteractive\n"
    script += "\n"

    script += 'if [ "' + install_deps_val + '" = "True" ]; then\n'
    script += "  sudo apt-get update\n"
    script += "  sudo apt-get install -y \\\n"
    script += "    build-essential \\\n"
    script += "    cmake \\\n"
    script += "    pkg-config \\\n"
    script += "    git \\\n"
    script += "    clang \\\n"
    script += "    gdb \\\n"
    script += "    make \\\n"
    script += "    fio \\\n"
    script += "    jq \\\n"
    script += "    python3 \\\n"
    script += "    python3-pip \\\n"
    script += "    fuse3 \\\n"
    script += "    libfuse3-dev \\\n"
    script += "    xfsprogs \\\n"
    script += "    e2fsprogs \\\n"
    script += "    rsync \\\n"
    script += "    net-tools \\\n"
    script += "    iperf3 \\\n"
    script += "    util-linux\n"
    script += "fi\n"
    script += "\n"

    script += 'command -v mount >/dev/null 2>&1 || { echo "mount missing"; exit 1; }\n'
    script += 'command -v mountpoint >/dev/null 2>&1 || { echo "mountpoint missing"; exit 1; }\n'
    script += 'command -v findmnt >/dev/null 2>&1 || { echo "findmnt missing"; exit 1; }\n'
    script += 'command -v blkid >/dev/null 2>&1 || { echo "blkid missing"; exit 1; }\n'
    script += 'command -v mkfs.ext4 >/dev/null 2>&1 || { echo "mkfs.ext4 missing"; exit 1; }\n'
    script += 'command -v mkfs.xfs >/dev/null 2>&1 || { echo "mkfs.xfs missing"; exit 1; }\n'
    script += 'command -v losetup >/dev/null 2>&1 || { echo "losetup missing"; exit 1; }\n'
    script += "\n"

    script += "CLOUDLAB_USER_OVERRIDE='" + cloudlab_user_val + "'\n"
    script += 'if [ -n "$CLOUDLAB_USER_OVERRIDE" ]; then\n'
    script += '  CLOUDLAB_USER="$CLOUDLAB_USER_OVERRIDE"\n'
    script += 'elif [ -d /users ] && [ -n "$(find /users -mindepth 1 -maxdepth 1 -type d | head -n1)" ]; then\n'
    script += '  CLOUDLAB_USER="$(find /users -mindepth 1 -maxdepth 1 -type d -printf "%f\\n" | head -n1)"\n'
    script += "else\n"
    script += '  CLOUDLAB_USER="${SUDO_USER:-${USER}}"\n'
    script += "fi\n"
    script += "\n"

    script += 'USER_BASE="/users/${CLOUDLAB_USER}"\n'
    script += 'mkdir -p "${USER_BASE}/results"\n'
    script += 'mkdir -p "${USER_BASE}/bin"\n'
    script += 'mkdir -p "${USER_BASE}/mux-config"\n'
    script += 'mkdir -p "${USER_BASE}/mux-scripts"\n'
    script += 'mkdir -p "${USER_BASE}/workloads"\n'
    script += "\n"

    if params.repo_url.strip():
        script += 'cd "${USER_BASE}"\n'
        script += 'if [ ! -d AsyncMux ]; then\n'
        script += "  git clone -b '" + repo_branch_val + "' '" + repo_url_val + "' AsyncMux\n"
        script += "fi\n"
        script += "\n"

    script += "WORKSPACE_DIR='" + workspace_dir_val + "'\n"
    script += "NUM_TIERS=" + num_tiers_val + "\n"
    script += "TIER0_TMPFS_GB=" + tier0_tmpfs_gb_val + "\n"
    script += "TIER1_SIZE_GB=" + tier1_size_gb_val + "\n"
    script += "TIER2_SIZE_GB=" + tier2_size_gb_val + "\n"
    script += "TIER3_SIZE_GB=" + tier3_size_gb_val + "\n"
    script += "TIER1_FS=" + tier1_fs_val + "\n"
    script += "TIER2_FS=" + tier2_fs_val + "\n"
    script += "TIER3_FS=" + tier3_fs_val + "\n"
    script += 'sudo mkdir -p "$WORKSPACE_DIR"\n'
    script += "\n"

    script += "remove_fstab_entry() {\n"
    script += '  local mountpoint="$1"\n'
    script += '  sudo sed -i "\\|[[:space:]]$mountpoint[[:space:]]|d" /etc/fstab\n'
    script += "}\n"
    script += "\n"

    script += "wait_for_mountpoint() {\n"
    script += '  local mountpoint="$1"\n'
    script += "  local tries=10\n"
    script += "  local i=0\n"
    script += '  while [ "$i" -lt "$tries" ]; do\n'
    script += '    if mountpoint -q "$mountpoint"; then\n'
    script += "      return 0\n"
    script += "    fi\n"
    script += "    sleep 1\n"
    script += '    i=$((i + 1))\n'
    script += "  done\n"
    script += '  echo "ERROR: timed out waiting for mountpoint $mountpoint"\n'
    script += "  return 1\n"
    script += "}\n"
    script += "\n"

    script += "cleanup_loop_for_image() {\n"
    script += '  local image_path="$1"\n'
    script += '  sudo losetup -j "$image_path" | cut -d: -f1 | while IFS= read -r dev; do\n'
    script += '    [ -n "$dev" ] || continue\n'
    script += '    sudo losetup -d "$dev" || true\n'
    script += "  done\n"
    script += "}\n"
    script += "\n"

    script += "setup_tmpfs_tier() {\n"
    script += '  local mountpoint="$1"\n'
    script += '  local size_gb="$2"\n'
    script += '  sudo mkdir -p "$mountpoint"\n'
    script += '  if mountpoint -q "$mountpoint"; then\n'
    script += '    sudo umount "$mountpoint" || true\n'
    script += "  fi\n"
    script += '  remove_fstab_entry "$mountpoint"\n'
    script += '  if [ "$size_gb" -gt 0 ]; then\n'
    script += '    sudo mount -t tmpfs -o size="${size_gb}G" tmpfs "$mountpoint"\n'
    script += '    wait_for_mountpoint "$mountpoint"\n'
    script += '    echo "tmpfs $mountpoint tmpfs rw,nosuid,nodev,size=${size_gb}G 0 0" | sudo tee -a /etc/fstab >/dev/null\n'
    script += '    findmnt "$mountpoint"\n'
    script += "  fi\n"
    script += "}\n"
    script += "\n"

    script += "setup_loop_fs_tier() {\n"
    script += '  local mount_base="$1"\n'
    script += '  local image_path="$2"\n'
    script += '  local size_gb="$3"\n'
    script += '  local fs_type="$4"\n'
    script += '  local data_mount="${mount_base}/data"\n'
    script += '  local loopdev=""\n'
    script += '  local current_type=""\n'
    script += "\n"
    script += '  sudo mkdir -p "$mount_base"\n'
    script += '  sudo mkdir -p "$data_mount"\n'
    script += '  sudo mkdir -p "${mount_base}/logs"\n'
    script += '  sudo mkdir -p "${mount_base}/cache"\n'
    script += '  sudo mkdir -p "${mount_base}/meta"\n'
    script += "\n"
    script += '  if mountpoint -q "$data_mount"; then\n'
    script += '    sudo umount "$data_mount" || true\n'
    script += "  fi\n"
    script += '  remove_fstab_entry "$data_mount"\n'
    script += '  cleanup_loop_for_image "$image_path"\n'
    script += "\n"
    script += '  sudo mkdir -p "$(dirname "$image_path")"\n'
    script += '  sudo truncate -s "${size_gb}G" "$image_path"\n'
    script += '  loopdev="$(sudo losetup --find --show "$image_path")"\n'
    script += "\n"
    script += '  if sudo blkid -o value -s TYPE "$loopdev" >/dev/null 2>&1; then\n'
    script += '    current_type="$(sudo blkid -o value -s TYPE "$loopdev")"\n'
    script += "  fi\n"
    script += "\n"
    script += '  if [ "$current_type" != "$fs_type" ]; then\n'
    script += '    printf "Formatting %s as %s (was: %s)\\n" "$loopdev" "$fs_type" "${current_type:-none}"\n'
    script += '    if [ "$fs_type" = "ext4" ]; then\n'
    script += '      sudo mkfs.ext4 -F "$loopdev"\n'
    script += '    elif [ "$fs_type" = "xfs" ]; then\n'
    script += '      sudo mkfs.xfs -f "$loopdev"\n'
    script += "    else\n"
    script += '      echo "Unsupported filesystem type: $fs_type"\n'
    script += '      sudo losetup -d "$loopdev" || true\n'
    script += "      exit 1\n"
    script += "    fi\n"
    script += "  fi\n"
    script += "\n"
    script += '  sudo mount -t "$fs_type" "$loopdev" "$data_mount"\n'
    script += '  wait_for_mountpoint "$data_mount"\n'
    script += "\n"
    script += "  local detected_type\n"
    script += '  detected_type="$(findmnt -n -o FSTYPE --target "$data_mount")"\n'
    script += '  if [ "$detected_type" != "$fs_type" ]; then\n'
    script += '    echo "ERROR: mounted $data_mount as $detected_type but expected $fs_type"\n'
    script += '    sudo umount "$data_mount" || true\n'
    script += '    sudo losetup -d "$loopdev" || true\n'
    script += "    exit 1\n"
    script += "  fi\n"
    script += "\n"
    script += '  echo "$image_path $data_mount $fs_type loop,rw,nosuid,nodev 0 0" | sudo tee -a /etc/fstab >/dev/null\n'
    script += '  echo "=== mounted $data_mount ==="\n'
    script += '  findmnt "$data_mount" || true\n'
    script += '  mount | grep "$data_mount" || true\n'
    script += '  stat -f -c "%n %T %t" "$data_mount" || true\n'
    script += '  sudo blkid "$image_path" || true\n'
    script += "}\n"
    script += "\n"

    script += "write_tier_conf() {\n"
    script += '  local tier_idx="$1"\n'
    script += '  local mount_base="$2"\n'
    script += '  local data_mount="$3"\n'
    script += '  local fs_type="$4"\n'
    script += '  local image_path="$5"\n'
    script += '  local tmpfs_mount="$6"\n'
    script += '  cat <<EOF | sudo tee "${mount_base}/tier.conf" >/dev/null\n'
    script += "tier_index=${tier_idx}\n"
    script += "data_mount=${data_mount}\n"
    script += "fs_type=${fs_type}\n"
    script += "image_path=${image_path}\n"
    script += "tmpfs_mount=${tmpfs_mount}\n"
    script += "EOF\n"
    script += "}\n"
    script += "\n"

    script += "sudo mkdir -p /tier0 /tier1 /tier2 /tier3\n"
    script += "sudo mkdir -p /tier0/logs /tier0/cache /tier0/meta\n"
    script += "sudo mkdir -p /tier1/logs /tier1/cache /tier1/meta\n"
    script += "sudo mkdir -p /tier2/logs /tier2/cache /tier2/meta\n"
    script += "sudo mkdir -p /tier3/logs /tier3/cache /tier3/meta\n"
    script += "\n"

    script += 'if [ "$TIER0_TMPFS_GB" -gt 0 ]; then\n'
    script += '  setup_tmpfs_tier /tier0/tmpfs "$TIER0_TMPFS_GB"\n'
    script += '  write_tier_conf 0 /tier0 "" "tmpfs" "" "/tier0/tmpfs"\n'
    script += "else\n"
    script += '  write_tier_conf 0 /tier0 "" "disabled" "" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'if [ "$NUM_TIERS" -ge 2 ]; then\n'
    script += '  setup_loop_fs_tier /tier1 "$WORKSPACE_DIR/tier1.img" "$TIER1_SIZE_GB" "$TIER1_FS"\n'
    script += '  write_tier_conf 1 /tier1 /tier1/data "$TIER1_FS" "$WORKSPACE_DIR/tier1.img" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'if [ "$NUM_TIERS" -ge 3 ]; then\n'
    script += '  setup_loop_fs_tier /tier2 "$WORKSPACE_DIR/tier2.img" "$TIER2_SIZE_GB" "$TIER2_FS"\n'
    script += '  write_tier_conf 2 /tier2 /tier2/data "$TIER2_FS" "$WORKSPACE_DIR/tier2.img" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'if [ "$NUM_TIERS" -ge 4 ]; then\n'
    script += '  setup_loop_fs_tier /tier3 "$WORKSPACE_DIR/tier3.img" "$TIER3_SIZE_GB" "$TIER3_FS"\n'
    script += '  write_tier_conf 3 /tier3 /tier3/data "$TIER3_FS" "$WORKSPACE_DIR/tier3.img" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'cat > "${USER_BASE}/mux-config/topology.env" <<EOF\n'
    script += "CLOUDLAB_USER=$CLOUDLAB_USER\n"
    script += "USER_BASE=$USER_BASE\n"
    script += "NUM_TIERS=$NUM_TIERS\n"
    script += "TIER0_TMPFS=/tier0/tmpfs\n"
    script += "TIER1_DATA=/tier1/data\n"
    script += "TIER2_DATA=/tier2/data\n"
    script += "TIER3_DATA=/tier3/data\n"
    script += "TIER1_FS=$TIER1_FS\n"
    script += "TIER2_FS=$TIER2_FS\n"
    script += "TIER3_FS=$TIER3_FS\n"
    script += "WORKSPACE_DIR=$WORKSPACE_DIR\n"
    script += "EOF\n"
    script += "\n"

    script += 'cat > "${USER_BASE}/mux-scripts/show-topology.sh" <<EOF\n'
    script += "#!/usr/bin/env bash\n"
    script += "set -euo pipefail\n"
    script += 'echo "Node: $(hostname)"\n'
    script += "echo\n"
    script += 'echo "Mounted tiers:"\n'
    script += "mount | grep /tier || true\n"
    script += "echo\n"
    script += 'echo "findmnt:"\n'
    script += "findmnt /tier0/tmpfs || true\n"
    script += "findmnt /tier1/data  || true\n"
    script += "findmnt /tier2/data  || true\n"
    script += "findmnt /tier3/data  || true\n"
    script += "echo\n"
    script += 'echo "Tier configs:"\n'
    script += "find /tier0 /tier1 /tier2 /tier3 -name tier.conf 2>/dev/null -print -exec cat {} \\;\n"
    script += "EOF\n"
    script += 'chmod +x "${USER_BASE}/mux-scripts/show-topology.sh"\n'
    script += "\n"

    script += 'cat > "${USER_BASE}/mux-scripts/debug-mounts.sh" <<EOF\n'
    script += "#!/usr/bin/env bash\n"
    script += "set -euo pipefail\n"
    script += 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\n'
    script += 'USER_BASE="$(cd "${SCRIPT_DIR}/.." && pwd)"\n'
    script += 'source "${USER_BASE}/mux-config/topology.env"\n'
    script += 'echo "=== mount ==="\n'
    script += "mount | grep /tier || true\n"
    script += "echo\n"
    script += 'echo "=== findmnt ==="\n'
    script += "findmnt /tier0/tmpfs || true\n"
    script += "findmnt /tier1/data  || true\n"
    script += "findmnt /tier2/data  || true\n"
    script += "findmnt /tier3/data  || true\n"
    script += "echo\n"
    script += 'echo "=== statfs ==="\n'
    script += 'stat -f -c "%n %T %t" /tier0/tmpfs || true\n'
    script += 'stat -f -c "%n %T %t" /tier1/data  || true\n'
    script += 'stat -f -c "%n %T %t" /tier2/data  || true\n'
    script += 'stat -f -c "%n %T %t" /tier3/data  || true\n'
    script += "echo\n"
    script += 'echo "=== blkid ==="\n'
    script += 'sudo blkid "$WORKSPACE_DIR/tier1.img" || true\n'
    script += 'sudo blkid "$WORKSPACE_DIR/tier2.img" || true\n'
    script += 'sudo blkid "$WORKSPACE_DIR/tier3.img" || true\n'
    script += "EOF\n"
    script += 'chmod +x "${USER_BASE}/mux-scripts/debug-mounts.sh"\n'
    script += "\n"

    script += 'cat > "${USER_BASE}/workloads/smoke.sh" <<EOF\n'
    script += "#!/usr/bin/env bash\n"
    script += "set -euo pipefail\n"
    script += 'echo "Single-node smoke test from $(hostname)"\n'
    script += "cmake --version\n"
    script += "fio --version || true\n"
    script += "mount | grep /tier || true\n"
    script += "findmnt /tier0/tmpfs || true\n"
    script += "findmnt /tier1/data  || true\n"
    script += "findmnt /tier2/data  || true\n"
    script += "findmnt /tier3/data  || true\n"
    script += "EOF\n"
    script += 'chmod +x "${USER_BASE}/workloads/smoke.sh"\n'
    script += "\n"

    script += 'cat > "${USER_BASE}/README-CLOUDLAB-TOPOLOGY.txt" <<EOF\n'
    script += "Single-node CloudLab multitier testbed\n"
    script += "\n"
    script += "Expected mounts:\n"
    script += "  /tier0/tmpfs  : optional tmpfs tier\n"
    script += "  /tier1/data   : image-backed filesystem ($TIER1_FS)\n"
    script += "  /tier2/data   : image-backed filesystem ($TIER2_FS)\n"
    script += "  /tier3/data   : image-backed filesystem ($TIER3_FS, if enabled)\n"
    script += "\n"
    script += "This profile is intended to support:\n"
    script += "  - AsyncMux single-host development\n"
    script += "  - multi-filesystem correctness tests\n"
    script += "  - tier placement/migration/promotion on one node\n"
    script += "EOF\n"
    script += "\n"

    script += 'echo "Single-node multi-filesystem setup complete on $(hostname)"\n'
    script += 'echo "Using CloudLab user: $CLOUDLAB_USER"\n'
    script += 'echo "==== FINAL TIER STATUS ===="\n'
    script += "mount | grep /tier || true\n"
    script += "findmnt /tier0/tmpfs || true\n"
    script += "findmnt /tier1/data  || true\n"
    script += "findmnt /tier2/data  || true\n"
    script += "findmnt /tier3/data  || true\n"
    script += 'stat -f -c "%n %T %t" /tier0/tmpfs || true\n'
    script += 'stat -f -c "%n %T %t" /tier1/data  || true\n'
    script += 'stat -f -c "%n %T %t" /tier2/data  || true\n'
    script += 'stat -f -c "%n %T %t" /tier3/data  || true\n'

    return script


def add_common_services(node):
    boot = combined_boot_script()
    boot_b64 = base64.b64encode(boot.encode("utf-8")).decode("ascii")
    launcher = (
        "python3 - <<'EOF'\n"
        "import base64\n"
        "data = " + repr(boot_b64) + "\n"
        "open('/tmp/cloudlab-mux-boot.sh', 'wb').write(base64.b64decode(data))\n"
        "EOF\n"
        "chmod +x /tmp/cloudlab-mux-boot.sh\n"
        "bash /tmp/cloudlab-mux-boot.sh\n"
    )
    node.addService(pg.Execute(shell="bash", command=launcher))


node = request.RawPC("muxnode")
apply_node_defaults(node)
add_common_services(node)

pc.printRequestRSpec(request)