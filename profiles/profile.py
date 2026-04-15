"""CloudLab profile for a prospective multi-tier Mux/AsyncMux testbed.

Roles:
- controller: orchestration, metadata, policy runner, experiment scripts
- client: workload generator / benchmark driver
- tier nodes: storage tiers, each with local filesystem-backed storage

This profile supports:
1) Co-located mode for current single-host AsyncMux development
2) Multi-node mode for future Mux-like / distributed experiments

Notes:
- Uses RawPC nodes.
- Each tier node can have:
    * a blockstore mounted at /tier-data
    * an optional tmpfs mounted at /tier-tmpfs
- All nodes are connected by a private LAN.
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
    "Number of storage tiers",
    portal.ParameterType.INTEGER,
    3,
    legalValues=[2, 3, 4]
)

pc.defineParameter(
    "separate_controller",
    "Use a dedicated controller node",
    portal.ParameterType.BOOLEAN,
    False
)

pc.defineParameter(
    "separate_client",
    "Use a dedicated client node",
    portal.ParameterType.BOOLEAN,
    False
)

pc.defineParameter(
    "colocate_mux_on_tier0",
    "Co-locate controller/Mux on tier0 node when possible",
    portal.ParameterType.BOOLEAN,
    True
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
    ""
)

pc.defineParameter(
    "repo_branch",
    "Optional git branch",
    portal.ParameterType.STRING,
    "main"
)

pc.defineParameter(
    "lan_bandwidth",
    "Private LAN bandwidth in kbps",
    portal.ParameterType.INTEGER,
    10000000
)

pc.defineParameter(
    "tier0_blockstore_gb",
    "Tier0 blockstore size in GB",
    portal.ParameterType.INTEGER,
    20
)

pc.defineParameter(
    "tier1_blockstore_gb",
    "Tier1 blockstore size in GB",
    portal.ParameterType.INTEGER,
    50
)

pc.defineParameter(
    "tier2_blockstore_gb",
    "Tier2 blockstore size in GB",
    portal.ParameterType.INTEGER,
    100
)

pc.defineParameter(
    "tier3_blockstore_gb",
    "Tier3 blockstore size in GB",
    portal.ParameterType.INTEGER,
    200
)

pc.defineParameter(
    "tier0_tmpfs_gb",
    "Tier0 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    4
)

pc.defineParameter(
    "tier1_tmpfs_gb",
    "Tier1 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    0
)

pc.defineParameter(
    "tier2_tmpfs_gb",
    "Tier2 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    0
)

pc.defineParameter(
    "tier3_tmpfs_gb",
    "Tier3 tmpfs size in GB (0 disables tmpfs)",
    portal.ParameterType.INTEGER,
    0
)

params = pc.bindParameters()

#
# Helpers
#

def apply_node_defaults(node):
    if params.hardware_type.strip():
        node.hardware_type = params.hardware_type.strip()
    node.disk_image = params.disk_image

def install_script():
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

    return r'''
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

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
  iperf3

mkdir -p /users/$USER/results
mkdir -p /users/$USER/bin

''' + repo_logic + r'''

echo "Bootstrap complete on $(hostname)"
'''

def setup_tier_script(tier_idx, tmpfs_gb):
        block_dev = "/dev/sdb"
        mount_base = "/tier{}".format(tier_idx)
        data_mount = "{}/data".format(mount_base)
        tmpfs_mount = "{}/tmpfs".format(mount_base)
        block_fs_marker = "{}/.blockstore_ready".format(mount_base)

        tmpfs_part = ""
        if tmpfs_gb > 0:
                tmpfs_part = '''
sudo mkdir -p {tmpfs_mount}
if ! mountpoint -q {tmpfs_mount}; then
    sudo mount -t tmpfs -o size={tmpfs_gb}G tmpfs {tmpfs_mount}
fi
if ! grep -q "{tmpfs_mount} " /etc/fstab; then
    echo "tmpfs {tmpfs_mount} tmpfs size={tmpfs_gb}G 0 0" | sudo tee -a /etc/fstab
fi
'''.format(tmpfs_mount=tmpfs_mount, tmpfs_gb=tmpfs_gb)

        tmpfs_conf_mount = tmpfs_mount if tmpfs_gb > 0 else ""

        return '''
set -euxo pipefail

sudo mkdir -p {mount_base}
sudo mkdir -p {data_mount}

if [ -b {block_dev} ]; then
    if ! sudo blkid {block_dev}; then
        sudo mkfs.ext4 -F {block_dev}
    fi

    if ! mountpoint -q {data_mount}; then
        sudo mount {block_dev} {data_mount}
    fi

    if ! grep -q "{data_mount} " /etc/fstab; then
        echo "{block_dev} {data_mount} ext4 defaults 0 0" | sudo tee -a /etc/fstab
    fi

    sudo touch {block_fs_marker}
fi

{tmpfs_part}

sudo mkdir -p {mount_base}/logs
sudo mkdir -p {mount_base}/cache
sudo mkdir -p {mount_base}/meta

echo "tier_index={tier_idx}" | sudo tee {mount_base}/tier.conf
echo "data_mount={data_mount}" | sudo tee -a {mount_base}/tier.conf
echo "tmpfs_mount={tmpfs_conf_mount}" | sudo tee -a {mount_base}/tier.conf

echo "Tier {tier_idx} configured on $(hostname)"
mount | grep "{mount_base}" || true
'''.format(
                mount_base=mount_base,
                data_mount=data_mount,
                block_dev=block_dev,
                block_fs_marker=block_fs_marker,
                tmpfs_part=tmpfs_part,
                tier_idx=tier_idx,
                tmpfs_conf_mount=tmpfs_conf_mount,
        )

def setup_controller_script(num_tiers):
    return '''
set -euxo pipefail

mkdir -p /users/$USER/mux-config
mkdir -p /users/$USER/mux-scripts
mkdir -p /users/$USER/results

cat > /users/$USER/mux-config/topology.env <<'EOF'
NUM_TIERS={num_tiers}
EOF

cat > /users/$USER/mux-scripts/show-topology.sh <<'EOF'
#!/bin/bash
set -euo pipefail
echo "Controller: $(hostname)"
echo "Configured tiers: ${{NUM_TIERS:-unknown}}"
EOF
chmod +x /users/$USER/mux-scripts/show-topology.sh

echo "Controller configured on $(hostname)"
'''.format(num_tiers=num_tiers)

def setup_client_script():
    return r'''
set -euxo pipefail

mkdir -p /users/$USER/workloads
mkdir -p /users/$USER/results

cat > /users/$USER/workloads/smoke.sh <<'EOF'
#!/bin/bash
set -euo pipefail
echo "Client smoke test from $(hostname)"
iperf3 --version >/dev/null 2>&1 || true
fio --version || true
EOF
chmod +x /users/$USER/workloads/smoke.sh

echo "Client configured on $(hostname)"
'''

def add_common_services(node, extra_script=None):
    if params.install_deps:
        node.addService(pg.Execute(shell="bash", command=install_script()))
    if extra_script:
        node.addService(pg.Execute(shell="bash", command=extra_script))

def make_tier_node(name, tier_idx, blockstore_gb, tmpfs_gb):
    node = request.RawPC(name)
    apply_node_defaults(node)

    if blockstore_gb > 0:
        bs = node.Blockstore("{}bs".format(name), "/unused")
        bs.size = "{}GB".format(blockstore_gb)

    add_common_services(node, setup_tier_script(tier_idx, tmpfs_gb))
    return node

#
# Build topology
#

tier_block_sizes = [
    params.tier0_blockstore_gb,
    params.tier1_blockstore_gb,
    params.tier2_blockstore_gb,
    params.tier3_blockstore_gb,
]

tier_tmpfs_sizes = [
    params.tier0_tmpfs_gb,
    params.tier1_tmpfs_gb,
    params.tier2_tmpfs_gb,
    params.tier3_tmpfs_gb,
]

lan = request.LAN("storage-lan")
lan.bandwidth = params.lan_bandwidth

tier_nodes = []
for i in range(params.num_tiers):
    tnode = make_tier_node(
        "tier{}".format(i),
        i,
        tier_block_sizes[i],
        tier_tmpfs_sizes[i]
    )
    iface = tnode.addInterface("if-tier{}".format(i))
    lan.addInterface(iface)
    tier_nodes.append(tnode)

controller_node = None
client_node = None

#
# Controller placement
#
if params.separate_controller:
    controller_node = request.RawPC("controller")
    apply_node_defaults(controller_node)
    add_common_services(controller_node, setup_controller_script(params.num_tiers))
    iface = controller_node.addInterface("if-controller")
    lan.addInterface(iface)
else:
    if params.colocate_mux_on_tier0:
        # Reuse tier0 as controller host via extra startup config.
        tier_nodes[0].addService(pg.Execute(shell="bash", command=setup_controller_script(params.num_tiers)))
        controller_node = tier_nodes[0]
    else:
        # Default: tier0 acts as controller if no separate controller requested.
        tier_nodes[0].addService(pg.Execute(shell="bash", command=setup_controller_script(params.num_tiers)))
        controller_node = tier_nodes[0]

#
# Client placement
#
if params.separate_client:
    client_node = request.RawPC("client")
    apply_node_defaults(client_node)
    add_common_services(client_node, setup_client_script())
    iface = client_node.addInterface("if-client")
    lan.addInterface(iface)
else:
    # Reuse controller as client driver if no dedicated client requested.
    controller_node.addService(pg.Execute(shell="bash", command=setup_client_script()))
    client_node = controller_node

#
# Optional notes file on controller/client
#
topology_note = '''
set -euxo pipefail
cat > /users/$USER/README-CLOUDLAB-TOPOLOGY.txt <<'EOF'
CloudLab multitier testbed

Roles:
- controller: orchestration / metadata / policy
- client: workload generation
- tier0..tier{num_tiers_max}: storage tiers

Expected per-tier mounts:
- /tierN/data   : blockstore-backed ext4
- /tierN/tmpfs  : optional RAM-backed tier

This profile is intended to support:
- current AsyncMux single-host style experiments
- future Mux-like multi-filesystem experiments
- eventual distributed Mux or RPC-based storage composition
EOF
'''.format(num_tiers_max=params.num_tiers - 1)
controller_node.addService(pg.Execute(shell="bash", command=topology_note))

pc.printRequestRSpec(request)