#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <sys/statfs.h>
#include <sys/wait.h>

#include "amux/asyncmux.hh"
#include "tests/utils.hh"

#include <cppcoro/sync_wait.hpp>

using asyncmux::AsyncMux;
using asyncmux::BlockId;
using asyncmux::BlockLocation;
using asyncmux::Byte;
using asyncmux::FileSystemTier;
using asyncmux::IoBuffer;
using asyncmux::MetadataStore;
using asyncmux::PlacementPolicy;
using asyncmux::TierId;
using asyncmux::TierRegistry;
using asyncmux::WriteBlock;
using asyncmux::kBlockSize;

using cppcoro::static_thread_pool;
using cppcoro::sync_wait;
using cppcoro::task;

namespace fs = std::filesystem;

namespace {

static_assert(
#ifdef __linux__
    true,
#else
    false,
#endif
    "multiple_fs.cc requires Linux for mount-based filesystem compatibility tests");

const fs::path kHotRoot = "/tmp/asyncmux_hot";
const fs::path kWarmRoot = "/tmp/asyncmux_warm";
const fs::path kColdRoot = "/tmp/asyncmux_cold";

const fs::path kWarmImage = "/tmp/asyncmux_warm.img";
const fs::path kColdImage = "/tmp/asyncmux_cold.img";

constexpr std::uint64_t kExt4ImageSizeMB = 128;

constexpr long kTmpfsMagic = 0x01021994;
constexpr long kExt4Magic = 0xEF53;

bool command_succeeds(const std::string& command) {
    const int rc = std::system(command.c_str());
    if (rc == -1) {
        return false;
    }
    return WIFEXITED(rc) && WEXITSTATUS(rc) == 0;
}

void require_command(const std::string& command) {
    if (!command_succeeds(command)) {
        throw std::runtime_error("required command failed: " + command);
    }
}

long fs_magic_for(const fs::path& root) {
    struct statfs stat_info {};
    if (statfs(root.c_str(), &stat_info) != 0) {
        throw std::runtime_error("statfs failed for " + root.string());
    }
    return static_cast<long>(stat_info.f_type);
}

bool is_mountpoint(const fs::path& root) {
    return command_succeeds("mountpoint -q " + root.string());
}

void ensure_dir(const fs::path& root) {
    std::error_code ec;
    fs::create_directories(root, ec);
    if (ec) {
        throw std::runtime_error("failed to create directory: " + root.string());
    }
}

void force_unmount_if_mounted(const fs::path& root) {
    if (is_mountpoint(root)) {
        require_command("umount " + root.string());
    }
}

void remove_file_if_exists(const fs::path& p) {
    std::error_code ec;
    fs::remove(p, ec);
}

void reset_dir(const fs::path& root) {
    std::error_code ec;
    fs::remove_all(root, ec);
    fs::create_directories(root, ec);
    if (ec) {
        throw std::runtime_error("failed to reset test directory");
    }
}

void mount_tmpfs_strict(const fs::path& root) {
    force_unmount_if_mounted(root);
    reset_dir(root);

    require_command("mount -t tmpfs -o size=512M tmpfs " + root.string());
    if (!is_mountpoint(root)) {
        throw std::runtime_error("tmpfs mount did not produce a mountpoint: " + root.string());
    }
    if (fs_magic_for(root) != kTmpfsMagic) {
        throw std::runtime_error("mounted hot tier is not tmpfs: " + root.string());
    }
}

void mount_ext4_image_strict(const fs::path& image, const fs::path& root) {
    force_unmount_if_mounted(root);
    reset_dir(root);
    remove_file_if_exists(image);

    require_command("dd if=/dev/zero of=" + image.string() +
                    " bs=1M count=" + std::to_string(kExt4ImageSizeMB) +
                    " status=none");
    require_command("mkfs.ext4 -F -q " + image.string());
    require_command("mount -o loop " + image.string() + " " + root.string());

    if (!is_mountpoint(root)) {
        throw std::runtime_error("ext4 mount did not produce a mountpoint: " + root.string());
    }
    if (fs_magic_for(root) != kExt4Magic) {
        throw std::runtime_error("mounted tier is not ext4: " + root.string());
    }
}

class MutablePlacementPolicy final : public PlacementPolicy {
public:
    explicit MutablePlacementPolicy(TierId tier) : tier_(tier) {}

    void set(TierId tier) {
        tier_ = tier;
    }

    TierId choose_tier(const WriteBlock&) override {
        return tier_;
    }

private:
    TierId tier_;
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

struct Fixture {
    static_thread_pool pool{4};
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};
    AsyncMux mux;

    Fixture()
        : mux(tiers, metadata, placement, pool, false) {
        ensure_dir(kHotRoot);
        ensure_dir(kWarmRoot);
        ensure_dir(kColdRoot);

        mount_tmpfs_strict(kHotRoot);
        mount_ext4_image_strict(kWarmImage, kWarmRoot);
        mount_ext4_image_strict(kColdImage, kColdRoot);

        tiers.add(std::make_unique<FileSystemTier>(1, "hot", kHotRoot, pool));
        tiers.add(std::make_unique<FileSystemTier>(2, "warm", kWarmRoot, pool));
        tiers.add(std::make_unique<FileSystemTier>(3, "cold", kColdRoot, pool));
    }

    ~Fixture() {
        std::system(("umount " + kHotRoot.string()).c_str());
        std::system(("umount " + kWarmRoot.string()).c_str());
        std::system(("umount " + kColdRoot.string()).c_str());
        remove_file_if_exists(kWarmImage);
        remove_file_if_exists(kColdImage);
    }
};

task<void> test_fs_types_are_expected(Fixture&) {
    assert_true(fs_magic_for(kHotRoot) == kTmpfsMagic,
                "hot tier must be tmpfs");
    assert_true(fs_magic_for(kWarmRoot) == kExt4Magic,
                "warm tier must be ext4");
    assert_true(fs_magic_for(kColdRoot) == kExt4Magic,
                "cold tier must be ext4");
    co_return;
}

task<void> test_placement_targets_expected_tier(Fixture& fx) {
    fx.placement.set(1);
    auto hot = to_bytes("hot");
    co_await fx.mux.write("/hot_file", 0,
                          std::span<const Byte>(hot.data(), hot.size()));

    fx.placement.set(2);
    auto warm = to_bytes("warm");
    co_await fx.mux.write("/warm_file", 0,
                          std::span<const Byte>(warm.data(), warm.size()));

    fx.placement.set(3);
    auto cold = to_bytes("cold");
    co_await fx.mux.write("/cold_file", 0,
                          std::span<const Byte>(cold.data(), cold.size()));

    const auto hot_loc = sorted_locs(fx.metadata, "/hot_file", 0, hot.size()).at(0);
    const auto warm_loc = sorted_locs(fx.metadata, "/warm_file", 0, warm.size()).at(0);
    const auto cold_loc = sorted_locs(fx.metadata, "/cold_file", 0, cold.size()).at(0);

    assert_true(hot_loc.tier_id == 1, "hot write should land on tier 1");
    assert_true(warm_loc.tier_id == 2, "warm write should land on tier 2");
    assert_true(cold_loc.tier_id == 3, "cold write should land on tier 3");

    assert_true(fs::exists(kHotRoot / "hot_file"), "hot root should contain hot_file");
    assert_true(fs::exists(kWarmRoot / "warm_file"), "warm root should contain warm_file");
    assert_true(fs::exists(kColdRoot / "cold_file"), "cold root should contain cold_file");

    fx.placement.set(1);
}

task<void> test_multi_tier_fanout_read(Fixture& fx) {
    std::string payload((2 * kBlockSize) + 250, 'x');
    for (int i = 0; i < 26; ++i) {
        payload[100 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 100 + i] = static_cast<char>('a' + i);
        payload[(2 * kBlockSize) + 100 + (i % 10)] = static_cast<char>('0' + (i % 10));
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/fanout", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/fanout", 0, bytes.size());
    assert_true(locs.size() >= 3, "fanout payload should span at least three blocks");

    co_await fx.mux.migrate(locs[0].block_id, 1, 2);
    co_await fx.mux.migrate(locs[1].block_id, 1, 3);

    assert_true(fx.metadata.tier_of(locs[0].block_id) == 2,
                "first block should migrate to warm tier");
    assert_true(fx.metadata.tier_of(locs[1].block_id) == 3,
                "second block should migrate to cold tier");

    IoBuffer all = co_await fx.mux.read("/fanout", 0, bytes.size());
    assert_eq(to_string(all), payload,
              "full fanout read should assemble bytes from multiple tiers");

    IoBuffer partial = co_await fx.mux.read("/fanout", kBlockSize - 12, 40);
    assert_eq(to_string(partial), payload.substr(kBlockSize - 12, 40),
              "partial fanout read should match expected substring");
}

task<void> test_promote_restores_hot_tier(Fixture& fx) {
    std::string payload(10000, 'p');
    for (int i = 0; i < 26; ++i) {
        payload[200 + i] = static_cast<char>('A' + i);
        payload[7000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/promote", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/promote", 0, bytes.size());
    assert_true(!locs.empty(), "promote payload should create block metadata");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        TierId dst = (i % 2 == 0) ? 2 : 3;
        co_await fx.mux.migrate(locs[i].block_id, 1, dst);
    }

    for (const auto& loc : locs) {
        co_await fx.mux.promote(loc.block_id, 1);
        assert_true(fx.metadata.tier_of(loc.block_id) == 1,
                    "promoted block should end on hot tier");
    }

    IoBuffer out = co_await fx.mux.read("/promote", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "promote should preserve bytes after moving blocks back to hot tier");
}

task<void> test_multiple_paths_across_tiers(Fixture& fx) {
    fx.placement.set(1);
    auto one = to_bytes("one-hot");
    co_await fx.mux.write("/group/one", 0,
                          std::span<const Byte>(one.data(), one.size()));

    fx.placement.set(2);
    auto two = to_bytes("two-warm");
    co_await fx.mux.write("/group/two", 0,
                          std::span<const Byte>(two.data(), two.size()));

    fx.placement.set(3);
    auto three = to_bytes("three-cold");
    co_await fx.mux.write("/group/three", 0,
                          std::span<const Byte>(three.data(), three.size()));

    IoBuffer out_one = co_await fx.mux.read("/group/one", 0, one.size());
    IoBuffer out_two = co_await fx.mux.read("/group/two", 0, two.size());
    IoBuffer out_three = co_await fx.mux.read("/group/three", 0, three.size());

    assert_eq(to_string(out_one), "one-hot", "first path should stay correct");
    assert_eq(to_string(out_two), "two-warm", "second path should stay correct");
    assert_eq(to_string(out_three), "three-cold", "third path should stay correct");

    assert_true(fs::exists(kHotRoot / "group" / "one"), "hot root should contain /group/one");
    assert_true(fs::exists(kWarmRoot / "group" / "two"), "warm root should contain /group/two");
    assert_true(fs::exists(kColdRoot / "group" / "three"), "cold root should contain /group/three");

    fx.placement.set(1);
}

task<void> test_missing_file_zero_fill(Fixture& fx) {
    IoBuffer out = co_await fx.mux.read("/missing", 0, 20);
    assert_true(out.size() == 20, "missing file read should return requested length");
    for (Byte b : out.data) {
        assert_true(b == Byte{0}, "missing file read should be zero-filled");
    }
}

task<void> run_all(Fixture& fx) {
    co_await test_fs_types_are_expected(fx);
    co_await test_placement_targets_expected_tier(fx);
    co_await test_multi_tier_fanout_read(fx);
    co_await test_promote_restores_hot_tier(fx);
    co_await test_multiple_paths_across_tiers(fx);
    co_await test_missing_file_zero_fill(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        sync_wait(run_all(fx));
        std::cout << kColorGreen
                  << "multiple_fs correctness tests passed."
                  << kColorReset << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << kColorRed << "FAIL:" << kColorReset
                  << " uncaught exception: " << ex.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << kColorRed << "FAIL:" << kColorReset
                  << " uncaught non-standard exception\n";
        return 1;
    }
}
