# AsyncMux

AsyncMux is an asynchronous multi-tier storage mux built on top of `cppcoro`. It splits file writes into 4 KiB blocks, stores block metadata, and routes reads and writes to one or more filesystem tiers. The project also includes a small FUSE-facing frontend wrapper and correctness tests for single-tier and multi-tier behavior.

## What It Does

- Splits writes into fixed-size blocks (`4096` bytes).
- Tracks block locations, tiers, and file extents in an in-memory metadata store.
- Reads data back from the correct tier or stitches together reads across multiple tiers.
- Supports block migration and promotion between tiers.
- Exposes a minimal `FuseFrontend` adapter for filesystem integration.

## Repository Layout

```text
.
├── amux/
│   ├── asyncmux.hh            # Umbrella public API header
│   ├── asyncmux.cc            # AsyncMux orchestration and FuseFrontend
│   ├── metadata_store.hh/.cc   # In-memory extent and block index store
│   ├── placement_policy.hh/.cc # Tier selection policies
│   ├── tier.hh/.cc             # Tier interface and registry
│   ├── block_allocator.hh      # Block id generator
│   └── tiers/
│       ├── filesystem_tier.hh  # Filesystem-backed tier interface
│       └── filesystem_tier.cc  # Filesystem-backed tier implementation
├── tests/
│   ├── single_fs.cc           # Single-tier correctness tests
│   ├── multiple_fs.cc         # Multi-tier mount-based correctness tests
│   └── utils.hh               # Test helpers and assertions
├── third_party/cppcoro/       # Vendored cppcoro dependency
├── Dockerfile                 # Build environment used by the Makefile
├── Makefile                   # Docker-based build and test commands
└── CMakeLists.txt             # CMake build configuration
```

## Prerequisites

- Docker
- Git submodules initialized, including `third_party/cppcoro`

If the `cppcoro` headers are missing, initialize the submodule with:

```bash
git submodule update --init --recursive
```

## Build

The project is built inside Docker.

```bash
make build
```

This builds the Docker image defined in [Dockerfile](Dockerfile) and compiles the project with CMake and Ninja inside the container.

## Run Tests

Run every discovered test binary:

```bash
make run
```

Run a specific test binary:

```bash
make run TEST=single_fs
make run TEST=multiple_fs
```

Test binaries are generated automatically from every `tests/*.cc` file, so the executable name matches the file stem.

### Test Notes

- `single_fs` exercises the core read/write path against one filesystem tier.
- `multiple_fs` mounts and verifies several filesystems, so it requires a Linux environment with mount privileges. The Docker-based test run already uses `--privileged`.

## Clean

Remove the local CMake build output directory:

```bash
make clean
```

## Implementation Notes

- `AsyncMux` is the main orchestration layer. It lives in [amux/asyncmux.cc](amux/asyncmux.cc) and is declared by the umbrella header [amux/asyncmux.hh](amux/asyncmux.hh).
- Shared types and helpers live in [amux/asyncmux.hh](amux/asyncmux.hh) and [amux/asyncmux.cc](amux/asyncmux.cc).
- `MetadataStore` tracks per-file extents and block-to-path lookups in [amux/metadata_store.hh](amux/metadata_store.hh) and [amux/metadata_store.cc](amux/metadata_store.cc).
- Placement is controlled by policies in [amux/placement_policy.hh](amux/placement_policy.hh) and [amux/placement_policy.cc](amux/placement_policy.cc), which lets tests steer writes to specific tiers.
- `Tier` and `TierRegistry` live in [amux/tier.hh](amux/tier.hh) and [amux/tier.cc](amux/tier.cc).
- `FileSystemTier` stores file contents on a backing filesystem rooted at a tier directory, with its declaration in [amux/tiers/filesystem_tier.hh](amux/tiers/filesystem_tier.hh) and implementation in [amux/tiers/filesystem_tier.cc](amux/tiers/filesystem_tier.cc).

## Troubleshooting

- If the build fails with missing `cppcoro` headers, initialize submodules first.
- If `multiple_fs` fails, make sure you are running in a privileged Linux environment. That test mounts filesystems and will not work on macOS hosts.

