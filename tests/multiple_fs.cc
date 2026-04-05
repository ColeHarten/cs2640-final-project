static_assert(
#ifdef __linux__
    true,
#else
    false,
#endif
    "async_mux_mounted_filesystems_tests.cpp can only be compiled on Linux");

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <vector>
#include <sys/wait.h>

#include "amux/asyncmux.hh"
#include "tests/utils.hh"

#include <cppcoro/sync_wait.hpp>

/*
 * Strict mounted-filesystem integration tests for AsyncMux.
 *
 * This suite ENFORCES the following layout:
 *   tier 1 (hot)  -> tmpfs
 *   tier 2 (warm) -> ext4 loopback filesystem
 *   tier 3 (cold) -> ext4 loopback filesystem
 *
 * If this layout cannot be created, the fixture throws and the test binary fails.
 *
 * The mounted roots are:
 *   /tmp/asyncmux_hot
 *   /tmp/asyncmux_warm
 *   /tmp/asyncmux_cold
 *
 * The loopback images are:
 *   /tmp/asyncmux_warm.img
 *   /tmp/asyncmux_cold.img
 *
 * These tests require:
 *   - Linux
 *   - mount privileges
 *   - mkfs.ext4
 *   - loopback mounts
 */

using asyncmux::AsyncMux;
using asyncmux::BlockId;
using asyncmux::BlockLocation;
using asyncmux::Byte;
using asyncmux::IoBuffer;
using asyncmux::MetadataStore;
using asyncmux::PlacementPolicy;
using asyncmux::SegmentFileTier;
using asyncmux::TierId;
using asyncmux::TierRegistry;
using asyncmux::WriteBlock;

using cppcoro::static_thread_pool;
using cppcoro::sync_wait;
using cppcoro::task;

namespace fs = std::filesystem;

namespace {

const fs::path HOT_ROOT  = "/tmp/asyncmux_hot";
const fs::path WARM_ROOT = "/tmp/asyncmux_warm";
const fs::path COLD_ROOT = "/tmp/asyncmux_cold";

const fs::path WARM_IMG  = "/tmp/asyncmux_warm.img";
const fs::path COLD_IMG  = "/tmp/asyncmux_cold.img";

constexpr std::uint64_t kExt4ImageSizeMB = 256;

fs::path segment_path(const fs::path& root, std::uint64_t segment_id) {
    return root / ("segment_" + std::to_string(segment_id) + ".bin");
}

std::string run_command(const std::string& cmd) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (pipe == nullptr) {
        throw std::runtime_error("failed to run command: " + cmd);
    }

    char buffer[256];
    std::string output;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output += buffer;
    }

    const int rc = pclose(pipe);
    if (rc == -1) {
        throw std::runtime_error("pclose failed for command: " + cmd);
    }
    if (!WIFEXITED(rc)) {
        throw std::runtime_error("command did not exit normally: " + cmd);
    }
    if (WEXITSTATUS(rc) != 0) {
        throw std::runtime_error("command failed: " + cmd);
    }

    while (!output.empty() &&
           (output.back() == '\n' || output.back() == '\r' ||
            output.back() == ' '  || output.back() == '\t')) {
        output.pop_back();
    }
    return output;
}

bool command_succeeds(const std::string& cmd) {
    const int rc = std::system(cmd.c_str());
    if (rc == -1) {
        return false;
    }
    if (!WIFEXITED(rc)) {
        return false;
    }
    return WEXITSTATUS(rc) == 0;
}

bool is_mountpoint(const fs::path& root) {
    const std::string cmd = "mountpoint -q " + root.string();
    return command_succeeds(cmd);
}

void ensure_dir(const fs::path& root) {
    std::error_code ec;
    fs::create_directories(root, ec);
    if (ec) {
        throw std::runtime_error("failed to create directory: " + root.string());
    }
}

void clear_dir_if_not_mountpoint(const fs::path& root) {
    std::error_code ec;
    if (!is_mountpoint(root) && fs::exists(root, ec)) {
        fs::remove_all(root, ec);
        if (ec) {
            throw std::runtime_error("failed to clear directory: " + root.string());
        }
    }
    ensure_dir(root);
}

void force_unmount_if_mounted(const fs::path& root) {
    if (is_mountpoint(root)) {
        const std::string cmd = "umount " + root.string();
        if (!command_succeeds(cmd)) {
            throw std::runtime_error("failed to unmount existing mount: " + root.string());
        }
    }
}

void remove_file_if_exists(const fs::path& p) {
    std::error_code ec;
    fs::remove(p, ec);
}

std::string fs_type(const fs::path& p) {
    return run_command("stat -f -c %T " + p.string() + " 2>/dev/null");
}

bool is_ext_family(const std::string& t) {
    return t == "ext2/ext3" || t == "ext4";
}

void mount_tmpfs_strict(const fs::path& root) {
    force_unmount_if_mounted(root);
    clear_dir_if_not_mountpoint(root);

    const std::string cmd =
        "mount -t tmpfs -o size=512M tmpfs " + root.string();
    if (!command_succeeds(cmd)) {
        throw std::runtime_error("failed to mount tmpfs at " + root.string());
    }

    if (!is_mountpoint(root)) {
        throw std::runtime_error("path is not a mountpoint after tmpfs mount: " + root.string());
    }

    const std::string type = fs_type(root);
    if (type != "tmpfs") {
        throw std::runtime_error(
            "expected tmpfs after mounting " + root.string() + ", got: " + type);
    }
}

void create_ext4_image_and_mount(const fs::path& image, const fs::path& root) {
    force_unmount_if_mounted(root);
    clear_dir_if_not_mountpoint(root);
    remove_file_if_exists(image);

    {
        std::ostringstream dd;
        dd << "dd if=/dev/zero of=" << image.string()
           << " bs=1M count=" << kExt4ImageSizeMB
           << " status=none";
        if (!command_succeeds(dd.str())) {
            throw std::runtime_error("failed to create image file: " + image.string());
        }
    }

    {
        const std::string mkfs = "mkfs.ext4 -F -q " + image.string();
        if (!command_succeeds(mkfs)) {
            throw std::runtime_error("failed to format ext4 image: " + image.string());
        }
    }

    {
        const std::string mount_cmd =
            "mount -o loop " + image.string() + " " + root.string();
        if (!command_succeeds(mount_cmd)) {
            throw std::runtime_error("failed to mount ext4 image at " + root.string());
        }
    }

    if (!is_mountpoint(root)) {
        throw std::runtime_error("path is not a mountpoint after ext4 mount: " + root.string());
    }

    const std::string type = fs_type(root);
    if (!is_ext_family(type)) {
        throw std::runtime_error(
            "expected ext4/ext-family after mounting " + root.string() + ", got: " + type);
    }
}

class MutablePlacementPolicy final : public PlacementPolicy {
public:
    explicit MutablePlacementPolicy(TierId tier) : tier_(tier) {}

    void set(TierId t) {
        tier_ = t;
    }

    TierId choose_tier(const WriteBlock&) override {
        return tier_;
    }

private:
    TierId tier_;
};

struct Fixture {
    static_thread_pool pool{4};
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};
    AsyncMux mux;

    std::string hot_fs;
    std::string warm_fs;
    std::string cold_fs;

    Fixture()
        : mux(tiers, metadata, placement, pool, false) {
        mount_tmpfs_strict(HOT_ROOT);
        create_ext4_image_and_mount(WARM_IMG, WARM_ROOT);
        create_ext4_image_and_mount(COLD_IMG, COLD_ROOT);

        hot_fs = fs_type(HOT_ROOT);
        warm_fs = fs_type(WARM_ROOT);
        cold_fs = fs_type(COLD_ROOT);

        // std::cerr << "hot_fs  = [" << hot_fs  << "]\n";
        // std::cerr << "warm_fs = [" << warm_fs << "]\n";
        // std::cerr << "cold_fs = [" << cold_fs << "]\n";

        if (hot_fs != "tmpfs") {
            throw std::runtime_error(
                "expected hot tier to be tmpfs, got: " + hot_fs);
        }
        if (!is_ext_family(warm_fs)) {
            throw std::runtime_error(
                "expected warm tier to be ext4/ext-family, got: " + warm_fs);
        }
        if (!is_ext_family(cold_fs)) {
            throw std::runtime_error(
                "expected cold tier to be ext4/ext-family, got: " + cold_fs);
        }

        tiers.add(std::make_unique<SegmentFileTier>(1, "hot", HOT_ROOT, pool));
        tiers.add(std::make_unique<SegmentFileTier>(2, "warm", WARM_ROOT, pool));
        tiers.add(std::make_unique<SegmentFileTier>(3, "cold", COLD_ROOT, pool));
    }

    ~Fixture() {
        std::system(("umount " + HOT_ROOT.string()).c_str());
        std::system(("umount " + WARM_ROOT.string()).c_str());
        std::system(("umount " + COLD_ROOT.string()).c_str());
        remove_file_if_exists(WARM_IMG);
        remove_file_if_exists(COLD_IMG);
    }
};

std::vector<BlockLocation> sorted_locs(MetadataStore& metadata,
                                       const std::string& path,
                                       std::uint64_t offset,
                                       std::uint64_t size) {
    auto locs = metadata.lookup(path, offset, size);
    std::sort(locs.begin(), locs.end(),
              [](const BlockLocation& a, const BlockLocation& b) {
                  return a.file_offset < b.file_offset;
              });
    return locs;
}

task<void> test_fs_types(Fixture& fx) {
    if (fx.hot_fs != "tmpfs") {
        throw std::runtime_error("hot tier must be tmpfs, got: " + fx.hot_fs);
    }
    if (!is_ext_family(fx.warm_fs)) {
        throw std::runtime_error("warm tier must be ext4/ext-family, got: " + fx.warm_fs);
    }
    if (!is_ext_family(fx.cold_fs)) {
        throw std::runtime_error("cold tier must be ext4/ext-family, got: " + fx.cold_fs);
    }
    co_return;
}

task<void> test_basic_rw(Fixture& fx) {
    auto bytes = to_bytes("hello");
    co_await fx.mux.write("/a", 0, {bytes.data(), bytes.size()});

    IoBuffer out = co_await fx.mux.read("/a", 0, bytes.size());
    assert_eq(to_string(out), "hello", "basic rw");
}

task<void> test_write_to_specific_tiers(Fixture& fx) {
    fx.placement.set(1);
    auto hot = to_bytes("hot-data");
    co_await fx.mux.write("/hot", 0, {hot.data(), hot.size()});

    fx.placement.set(2);
    auto warm = to_bytes("warm-data");
    co_await fx.mux.write("/warm", 0, {warm.data(), warm.size()});

    fx.placement.set(3);
    auto cold = to_bytes("cold-data");
    co_await fx.mux.write("/cold", 0, {cold.data(), cold.size()});

    auto hot_locs = sorted_locs(fx.metadata, "/hot", 0, hot.size());
    auto warm_locs = sorted_locs(fx.metadata, "/warm", 0, warm.size());
    auto cold_locs = sorted_locs(fx.metadata, "/cold", 0, cold.size());

    assert_true(hot_locs.size() == 1, "hot write should create one mapping");
    assert_true(warm_locs.size() == 1, "warm write should create one mapping");
    assert_true(cold_locs.size() == 1, "cold write should create one mapping");

    assert_true(hot_locs[0].tier_id == 1, "hot write should land on tier 1");
    assert_true(warm_locs[0].tier_id == 2, "warm write should land on tier 2");
    assert_true(cold_locs[0].tier_id == 3, "cold write should land on tier 3");

    assert_true(fs::exists(segment_path(HOT_ROOT, hot_locs[0].segment_id)),
                "hot segment should exist on tmpfs");
    assert_true(fs::exists(segment_path(WARM_ROOT, warm_locs[0].segment_id)),
                "warm segment should exist on ext4");
    assert_true(fs::exists(segment_path(COLD_ROOT, cold_locs[0].segment_id)),
                "cold segment should exist on ext4");

    IoBuffer hot_out = co_await fx.mux.read("/hot", 0, hot.size());
    IoBuffer warm_out = co_await fx.mux.read("/warm", 0, warm.size());
    IoBuffer cold_out = co_await fx.mux.read("/cold", 0, cold.size());

    assert_eq(to_string(hot_out), "hot-data", "tmpfs tier remains readable");
    assert_eq(to_string(warm_out), "warm-data", "warm ext4 tier remains readable");
    assert_eq(to_string(cold_out), "cold-data", "cold ext4 tier remains readable");

    fx.placement.set(1);
}

task<void> test_cross_tier_migration(Fixture& fx) {
    std::string payload(8000, 'x');
    auto bytes = to_bytes(payload);

    fx.placement.set(1);
    co_await fx.mux.write("/b", 0, {bytes.data(), bytes.size()});

    auto locs = sorted_locs(fx.metadata, "/b", 0, bytes.size());
    assert_true(!locs.empty(), "migration source file should have mappings");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        TierId dst = (i % 2 == 0) ? 2 : 3;
        co_await fx.mux.migrate(locs[i].block_id, 1, dst);
    }

    auto out = co_await fx.mux.read("/b", 0, bytes.size());
    assert_eq(to_string(out), payload, "migration preserves data");
}

task<void> test_fanout(Fixture& fx) {
    std::string payload(12000, 'z');
    auto bytes = to_bytes(payload);

    fx.placement.set(1);
    co_await fx.mux.write("/c", 0, {bytes.data(), bytes.size()});
    auto locs = sorted_locs(fx.metadata, "/c", 0, bytes.size());

    assert_true(locs.size() >= 3, "fanout file should span multiple mappings");

    co_await fx.mux.migrate(locs[0].block_id, 1, 2);
    co_await fx.mux.migrate(locs[1].block_id, 1, 3);

    auto out = co_await fx.mux.read("/c", 0, bytes.size());
    assert_eq(to_string(out), payload, "fanout read");

    auto partial = co_await fx.mux.read("/c", 4090, 6000);
    assert_eq(to_string(partial), payload.substr(4090, 6000),
              "partial fanout read");
}

task<void> test_promote_back_to_tmpfs(Fixture& fx) {
    std::string payload(10000, 'p');
    for (int i = 0; i < 26; ++i) {
        payload[100 + i] = static_cast<char>('A' + i);
        payload[7000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);

    fx.placement.set(1);
    co_await fx.mux.write("/promote", 0, {bytes.data(), bytes.size()});
    auto locs = sorted_locs(fx.metadata, "/promote", 0, bytes.size());

    assert_true(!locs.empty(), "promote file should have mappings");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        TierId dst = (i % 2 == 0) ? 2 : 3;
        co_await fx.mux.migrate(locs[i].block_id, 1, dst);
    }

    for (const auto& loc : locs) {
        TierId src = fx.metadata.tier_of(loc.block_id);
        if (src != 1) {
            co_await fx.mux.promote(loc.block_id, 1);
        }
    }

    auto out = co_await fx.mux.read("/promote", 0, bytes.size());
    assert_eq(to_string(out), payload, "promote back to tmpfs preserves data");
}

task<void> test_extent_metadata_targets_expected_roots(Fixture& fx) {
    fx.placement.set(1);
    auto hot = to_bytes("meta-hot");
    co_await fx.mux.write("/meta-hot", 0, {hot.data(), hot.size()});

    fx.placement.set(2);
    auto warm = to_bytes("meta-warm");
    co_await fx.mux.write("/meta-warm", 0, {warm.data(), warm.size()});

    fx.placement.set(3);
    auto cold = to_bytes("meta-cold");
    co_await fx.mux.write("/meta-cold", 0, {cold.data(), cold.size()});

    const auto hot_loc = sorted_locs(fx.metadata, "/meta-hot", 0, hot.size()).at(0);
    const auto warm_loc = sorted_locs(fx.metadata, "/meta-warm", 0, warm.size()).at(0);
    const auto cold_loc = sorted_locs(fx.metadata, "/meta-cold", 0, cold.size()).at(0);

    assert_true(fs::exists(segment_path(HOT_ROOT, hot_loc.segment_id)),
                "hot metadata should point into tmpfs root");
    assert_true(fs::exists(segment_path(WARM_ROOT, warm_loc.segment_id)),
                "warm metadata should point into ext4 warm root");
    assert_true(fs::exists(segment_path(COLD_ROOT, cold_loc.segment_id)),
                "cold metadata should point into ext4 cold root");

    fx.placement.set(1);
}

task<void> run(Fixture& fx) {
    co_await test_fs_types(fx);
    co_await test_basic_rw(fx);
    co_await test_write_to_specific_tiers(fx);
    co_await test_cross_tier_migration(fx);
    co_await test_fanout(fx);
    co_await test_promote_back_to_tmpfs(fx);
    co_await test_extent_metadata_targets_expected_roots(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        sync_wait(run(fx));
        std::cout << kColorGreen
                  << "Multiple filesystem multi-tier tests passed."
                  << kColorReset << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << kColorRed
                  << "FAIL: " << e.what()
                  << kColorReset << "\n";
        return 1;
    }
}