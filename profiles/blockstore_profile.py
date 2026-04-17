#!/usr/bin/env python3

"""
CloudLab Single-Node Multi-Blockstore AsyncMux Testbed
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
    "CloudLab username",
    portal.ParameterType.STRING,
    "hartenc",
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
    "",
)
pc.defineParameter(
    "repo_branch",
    "Optional git branch",
    portal.ParameterType.STRING,
    "main",
)
pc.defineParameter(
    "tier0_tmpfs_gb",
    "Tier0 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    4,
)
pc.defineParameter(
    "tier1_size_gb",
    "Tier1 blockstore size in GB",
    portal.ParameterType.INTEGER,
    20,
)
pc.defineParameter(
    "tier2_size_gb",
    "Tier2 blockstore size in GB",
    portal.ParameterType.INTEGER,
    50,
)
pc.defineParameter(
    "tier3_size_gb",
    "Tier3 blockstore size in GB",
    portal.ParameterType.INTEGER,
    100,
)

pc.defineParameter(
    "tier2_fs",
    "Tier2 filesystem type",
    portal.ParameterType.STRING,
    "xfs",
    legalValues=[("ext4", "ext4"), ("xfs", "xfs")],
)

params = pc.bindParameters()


def sq(s):
    return s.replace("'", "'\"'\"'")


def apply_node_defaults(node):
    if params.hardware_type.strip():
        node.hardware_type = params.hardware_type.strip()
    node.disk_image = params.disk_image


def add_blockstores(node):
    # CloudLab blockstores are requested on the node and mounted at the
    # specified mountpoint by the testbed.
    bs1 = node.Blockstore("tier1_bs", "/tier1/data")
    bs1.size = str(params.tier1_size_gb) + "GB"
    bs1.placement = "nonsysvol"

    if params.num_tiers >= 3:
        bs2 = node.Blockstore("tier2_bs", "/tier2/data")
        bs2.size = str(params.tier2_size_gb) + "GB"
        bs2.placement = "nonsysvol"

    if params.num_tiers >= 4:
        bs3 = node.Blockstore("tier3_bs", "/tier3/data")
        bs3.size = str(params.tier3_size_gb) + "GB"
        bs3.placement = "nonsysvol"


def combined_boot_script():
    install_deps_val = str(params.install_deps)
    repo_url_val = sq(params.repo_url)
    repo_branch_val = sq(params.repo_branch)
    cloudlab_user_val = sq(params.cloudlab_user)
    num_tiers_val = str(params.num_tiers)
    tier0_tmpfs_gb_val = str(params.tier0_tmpfs_gb)
    tier2_fs_val = sq(params.tier2_fs)

    script = "#!/usr/bin/env bash\n"
    script += "set -euxo pipefail\n"
    script += "\n"
    script += "export DEBIAN_FRONTEND=noninteractive\n"
    script += "\n"

    script += 'if [ "' + install_deps_val + '" = "True" ]; then\n'
    script += "  sudo apt-get update\n"
    script += "  sudo apt-get install -y \\\n"
    script += "    build-essential \\\n"
    script += "    g++ \\\n"
    script += "    clang-12 \\\n"
    script += "    libc++-12-dev \\\n"
    script += "    libc++abi-12-dev \\\n"
    script += "    libunwind-12-dev \\\n"
    script += "    cmake \\\n"
    script += "    pkg-config \\\n"
    script += "    git \\\n"
    script += "    gdb \\\n"
    script += "    make \\\n"
    script += "    fio \\\n"
    script += "    jq \\\n"
    script += "    python3 \\\n"
    script += "    python3-pip \\\n"
    script += "    fuse3 \\\n"
    script += "    libfuse3-dev \\\n"
    script += "    rsync \\\n"
    script += "    net-tools \\\n"
    script += "    iperf3 \\\n"
    script += "    util-linux \\\n"
    script += "    xfsprogs \\\n"
    script += "    e2fsprogs \\\n"
    script += "fi\n"
    script += "\n"

    script += 'command -v mount >/dev/null 2>&1 || { echo "mount missing"; exit 1; }\n'
    script += 'command -v mountpoint >/dev/null 2>&1 || { echo "mountpoint missing"; exit 1; }\n'
    script += 'command -v findmnt >/dev/null 2>&1 || { echo "findmnt missing"; exit 1; }\n'
    script += "\n"

    script += "CLOUDLAB_USER='" + cloudlab_user_val + "'\n"
    script += 'if [ -z "$CLOUDLAB_USER" ]; then\n'
    script += '  echo "cloudlab_user parameter must be provided"\n'
    script += "  exit 1\n"
    script += "fi\n"
    script += "\n"

    script += 'USER_BASE="/users/${CLOUDLAB_USER}"\n'
    script += 'sudo mkdir -p "${USER_BASE}/results"\n'
    script += 'sudo mkdir -p "${USER_BASE}/bin"\n'
    script += 'sudo mkdir -p "${USER_BASE}/mux-config"\n'
    script += 'sudo mkdir -p "${USER_BASE}/mux-scripts"\n'
    script += 'sudo mkdir -p "${USER_BASE}/workloads"\n'
    script += 'sudo chown -R "${CLOUDLAB_USER}" "${USER_BASE}/results" "${USER_BASE}/bin" "${USER_BASE}/mux-config" "${USER_BASE}/mux-scripts" "${USER_BASE}/workloads" || true\n'
    script += "\n"

    if params.repo_url.strip():
        script += 'cd "${USER_BASE}"\n'
        script += 'if [ ! -d AsyncMux ]; then\n'
        script += "  git clone -b '" + repo_branch_val + "' '" + repo_url_val + "' AsyncMux\n"
        script += "fi\n"
        script += "\n"

    script += "NUM_TIERS=" + num_tiers_val + "\n"
    script += "TIER0_TMPFS_GB=" + tier0_tmpfs_gb_val + "\n"
    script += "TIER2_FS='" + tier2_fs_val + "'\n"
    script += "\n"

    script += "wait_for_mountpoint() {\n"
    script += '  local mountpoint="$1"\n'
    script += "  local tries=30\n"
    script += "  local i=0\n"
    script += '  while [ "$i" -lt "$tries" ]; do\n'
    script += '    if mountpoint -q "$mountpoint"; then\n'
    script += "      return 0\n"
    script += "    fi\n"
    script += "    sleep 2\n"
    script += '    i=$((i + 1))\n'
    script += "  done\n"
    script += '  echo "ERROR: timed out waiting for mountpoint $mountpoint"\n'
    script += "  return 1\n"
    script += "}\n"
    script += "\n"

    script += "setup_tmpfs_tier() {\n"
    script += '  local mountpoint="$1"\n'
    script += '  local size_gb="$2"\n'
    script += '  sudo mkdir -p "$mountpoint"\n'
    script += '  if mountpoint -q "$mountpoint"; then\n'
    script += '    sudo umount "$mountpoint" || true\n'
    script += "  fi\n"
    script += '  if [ "$size_gb" -gt 0 ]; then\n'
    script += '    sudo mount -t tmpfs -o size="${size_gb}G" tmpfs "$mountpoint"\n'
    script += '    wait_for_mountpoint "$mountpoint"\n'
    script += '    sudo chown "${CLOUDLAB_USER}" "$mountpoint"\n'
    script += '    sudo chmod 755 "$mountpoint"\n'
    script += "  fi\n"
    script += "}\n"
    script += "\n"

    script += "prepare_tier_dir() {\n"
    script += '  local tier_root="$1"\n'
    script += '  local data_mount="$2"\n'
    script += '  sudo mkdir -p "$tier_root"\n'
    script += '  sudo mkdir -p "${tier_root}/logs"\n'
    script += '  sudo mkdir -p "${tier_root}/cache"\n'
    script += '  sudo mkdir -p "${tier_root}/meta"\n'
    script += '  if [ -n "$data_mount" ]; then\n'
    script += '    wait_for_mountpoint "$data_mount"\n'
    script += '    sudo chown "${CLOUDLAB_USER}" "$data_mount" || true\n'
    script += '    sudo chmod 755 "$data_mount" || true\n'
    script += "  fi\n"
    script += "}\n"
    script += "\n"

    script += "reformat_blockstore() {\n"
    script += '  local mountpoint="$1"\n'
    script += '  local fs_type="$2"\n'
    script += '  local dev=""\n'
    script += '  wait_for_mountpoint "$mountpoint"\n'
    script += '  dev="$(findmnt -n -o SOURCE --target "$mountpoint")"\n'
    script += '  if [ -z "$dev" ]; then\n'
    script += '    echo "ERROR: could not determine backing device for $mountpoint"\n'
    script += '    exit 1\n'
    script += "  fi\n"
    script += '  echo "Reformatting $dev mounted at $mountpoint as $fs_type"\n'
    script += '  sudo umount "$mountpoint"\n'
    script += '  if [ "$fs_type" = "xfs" ]; then\n'
    script += '    sudo mkfs.xfs -f "$dev"\n'
    script += '    sudo mount -t xfs "$dev" "$mountpoint"\n'
    script += '  elif [ "$fs_type" = "ext4" ]; then\n'
    script += '    sudo mkfs.ext4 -F "$dev"\n'
    script += '    sudo mount -t ext4 "$dev" "$mountpoint"\n'
    script += "  else\n"
    script += '    echo "ERROR: unsupported filesystem type $fs_type"\n'
    script += '    exit 1\n'
    script += "  fi\n"
    script += '  wait_for_mountpoint "$mountpoint"\n'
    script += '  sudo chown "${CLOUDLAB_USER}" "$mountpoint" || true\n'
    script += '  sudo chmod 755 "$mountpoint" || true\n'
    script += "}\n"
    script += "\n"

    script += "write_tier_conf() {\n"
    script += '  local tier_idx="$1"\n'
    script += '  local mount_base="$2"\n'
    script += '  local data_mount="$3"\n'
    script += '  local backing_kind="$4"\n'
    script += '  local tmpfs_mount="$5"\n'
    script += '  cat <<EOF | sudo tee "${mount_base}/tier.conf" >/dev/null\n'
    script += "tier_index=${tier_idx}\n"
    script += "data_mount=${data_mount}\n"
    script += "backing_kind=${backing_kind}\n"
    script += "tmpfs_mount=${tmpfs_mount}\n"
    script += "EOF\n"
    script += '  sudo chown "${CLOUDLAB_USER}" "${mount_base}/tier.conf"\n'
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
    script += '  write_tier_conf 0 /tier0 "" "tmpfs" "/tier0/tmpfs"\n'
    script += "else\n"
    script += '  write_tier_conf 0 /tier0 "" "disabled" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'prepare_tier_dir /tier1 /tier1/data\n'
    script += 'write_tier_conf 1 /tier1 /tier1/data "blockstore" ""\n'
    script += "\n"

    script += 'if [ "$NUM_TIERS" -ge 3 ]; then\n'
    script += '  prepare_tier_dir /tier2 /tier2/data\n'
    script += '  reformat_blockstore /tier2/data "$TIER2_FS"\n'
    script += '  write_tier_conf 2 /tier2 /tier2/data "blockstore" ""\n'
    script += "fi\n"

    script += 'if [ "$NUM_TIERS" -ge 4 ]; then\n'
    script += '  prepare_tier_dir /tier3 /tier3/data\n'
    script += '  write_tier_conf 3 /tier3 /tier3/data "blockstore" ""\n'
    script += "fi\n"
    script += "\n"

    script += 'cat <<EOF | sudo tee "${USER_BASE}/mux-config/topology.env" >/dev/null\n'
    script += "CLOUDLAB_USER=$CLOUDLAB_USER\n"
    script += "USER_BASE=$USER_BASE\n"
    script += "NUM_TIERS=$NUM_TIERS\n"
    script += "TIER0_TMPFS=/tier0/tmpfs\n"
    script += "TIER1_DATA=/tier1/data\n"
    script += "TIER2_DATA=/tier2/data\n"
    script += "TIER3_DATA=/tier3/data\n"
    script += "EOF\n"
    script += 'sudo chown "${CLOUDLAB_USER}" "${USER_BASE}/mux-config/topology.env"\n'
    script += 'sudo chmod 644 "${USER_BASE}/mux-config/topology.env"\n'
    script += "\n"

    script += 'cat <<\'EOF\' | sudo tee "${USER_BASE}/mux-scripts/show-topology.sh" >/dev/null\n'
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
    script += 'sudo chown "${CLOUDLAB_USER}" "${USER_BASE}/mux-scripts/show-topology.sh"\n'
    script += 'sudo chmod 755 "${USER_BASE}/mux-scripts/show-topology.sh"\n'
    script += "\n"

    script += 'cat <<\'EOF\' | sudo tee "${USER_BASE}/mux-scripts/debug-mounts.sh" >/dev/null\n'
    script += "#!/usr/bin/env bash\n"
    script += "set -euo pipefail\n"
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
    script += "EOF\n"
    script += 'sudo chown "${CLOUDLAB_USER}" "${USER_BASE}/mux-scripts/debug-mounts.sh"\n'
    script += 'sudo chmod 755 "${USER_BASE}/mux-scripts/debug-mounts.sh"\n'
    script += "\n"

    script += 'cat <<\'EOF\' | sudo tee "${USER_BASE}/workloads/smoke.sh" >/dev/null\n'
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
    script += 'sudo chown "${CLOUDLAB_USER}" "${USER_BASE}/workloads/smoke.sh"\n'
    script += 'sudo chmod 755 "${USER_BASE}/workloads/smoke.sh"\n'
    script += "\n"

    script += 'cat <<EOF | sudo tee "${USER_BASE}/README-CLOUDLAB-TOPOLOGY.txt" >/dev/null\n'
    script += "Single-node CloudLab multitier testbed with real blockstores\n"
    script += "\n"
    script += "Expected mounts:\n"
    script += "  /tier0/tmpfs  : optional tmpfs tier\n"
    script += "  /tier1/data   : blockstore-backed tier\n"
    script += "  /tier2/data   : blockstore-backed tier\n"
    script += "  /tier3/data   : blockstore-backed tier\n"
    script += "\n"
    script += "This profile is intended to support:\n"
    script += "  - AsyncMux single-host development\n"
    script += "  - multi-filesystem correctness tests\n"
    script += "  - tier placement/migration/promotion on one node\n"
    script += "EOF\n"
    script += 'sudo chown "${CLOUDLAB_USER}" "${USER_BASE}/README-CLOUDLAB-TOPOLOGY.txt"\n'
    script += 'sudo chmod 644 "${USER_BASE}/README-CLOUDLAB-TOPOLOGY.txt"\n'
    script += "\n"

    script += 'echo "Single-node multi-blockstore setup complete on $(hostname)"\n'
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
add_blockstores(node)
add_common_services(node)

pc.printRequestRSpec(request)