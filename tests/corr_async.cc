#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "amux/asyncmux.hh"
#include "tests/utils.hh"
#include "../span.hh"

#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

using amux::AsyncMux;
using amux::BlockLocation;
using amux::FileSystemTier;
using amux::IoBuffer;
using amux::MetadataStore;
using amux::MutablePlacementPolicy;
using amux::TierId;
using amux::TierRegistry;
using amux::kBlockSize;

using cppcoro::static_thread_pool;
using cppcoro::sync_wait;
using cppcoro::task;

namespace {

static_assert(
#ifdef __linux__
    true,
#else
    false,
#endif
    __FILE__ " requires Linux");

void ensure_tier_ready(const fs::path& root, const std::string& name) {
    ensure_dir_exists(root);

    std::cerr << "Checking " << name << " tier at " << root << "\n";
    std::cerr << "  exists: " << fs::exists(root) << "\n";
    std::cerr << "  is_dir: " << fs::is_directory(root) << "\n";
    std::cerr << "  is_mountpoint: " << is_mountpoint(root) << "\n";

    if (!is_mountpoint(root)) {
        throw std::runtime_error(name + " tier is not mounted: " + root.string());
    }

    std::cerr << "  fs_magic: 0x" << std::hex << fs_magic_for(root) << std::dec << "\n";

    if (::access(root.c_str(), W_OK) != 0) {
        throw std::runtime_error("tier root is not writable: " + root.string() +
                                 " errno=" + std::to_string(errno) +
                                 " (" + std::string(std::strerror(errno)) + ")");
    }
}

void assert_non_overlapping(const std::vector<BlockLocation>& locs,
                            const std::string& context) {
    for (std::size_t i = 1; i < locs.size(); ++i) {
        const auto prev_end = locs[i - 1].file_offset + locs[i - 1].size;
        assert_true(prev_end <= locs[i].file_offset,
                    context + ": metadata extents must not overlap");
    }
}

void cleanup_test_artifacts() {
    std::error_code ec;
    fs::remove(kHotRoot / "hot_file", ec);
    fs::remove(kWarmRoot / "hot_file", ec);
    fs::remove(kColdRoot / "hot_file", ec);

    fs::remove(kHotRoot / "warm_file", ec);
    fs::remove(kWarmRoot / "warm_file", ec);
    fs::remove(kColdRoot / "warm_file", ec);

    fs::remove(kHotRoot / "cold_file", ec);
    fs::remove(kWarmRoot / "cold_file", ec);
    fs::remove(kColdRoot / "cold_file", ec);

    fs::remove(kHotRoot / "fanout", ec);
    fs::remove(kWarmRoot / "fanout", ec);
    fs::remove(kColdRoot / "fanout", ec);

    fs::remove(kHotRoot / "promote", ec);
    fs::remove(kWarmRoot / "promote", ec);
    fs::remove(kColdRoot / "promote", ec);

    fs::remove(kHotRoot / "bg_promote", ec);
    fs::remove(kWarmRoot / "bg_promote", ec);
    fs::remove(kColdRoot / "bg_promote", ec);

    fs::remove(kHotRoot / "overwrite", ec);
    fs::remove(kWarmRoot / "overwrite", ec);
    fs::remove(kColdRoot / "overwrite", ec);

    fs::remove(kHotRoot / "cross_overwrite", ec);
    fs::remove(kWarmRoot / "cross_overwrite", ec);
    fs::remove(kColdRoot / "cross_overwrite", ec);

    fs::remove_all(kHotRoot / "group", ec);
    fs::remove_all(kWarmRoot / "group", ec);
    fs::remove_all(kColdRoot / "group", ec);
}

bool wait_until(std::function<bool()> pred,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(3000),
                std::chrono::milliseconds poll = std::chrono::milliseconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(poll);
    }
    return pred();
}

struct FixtureBase {
    static_thread_pool pool{8};
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};

    void init_common() {
        wait_for_mount(kHotRoot, "hot");
        wait_for_mount(kWarmRoot, "warm");
        wait_for_mount(kColdRoot, "cold");

        ensure_tier_ready(kHotRoot, "hot");
        ensure_tier_ready(kWarmRoot, "warm");
        ensure_tier_ready(kColdRoot, "cold");

        cleanup_test_artifacts();

        tiers.add(std::make_unique<FileSystemTier>(1, "hot", kHotRoot, pool));
        tiers.add(std::make_unique<FileSystemTier>(2, "warm", kWarmRoot, pool));
        tiers.add(std::make_unique<FileSystemTier>(3, "cold", kColdRoot, pool));
    }

    ~FixtureBase() {
        cleanup_test_artifacts();
    }
};

struct FixtureNoBg : public FixtureBase {
    std::unique_ptr<AsyncMux> mux;

    FixtureNoBg() {
        init_common();
        mux = std::make_unique<AsyncMux>(tiers, metadata, placement, pool, false, 1);
    }
};

struct FixtureBg : public FixtureBase {
    std::unique_ptr<AsyncMux> mux;

    FixtureBg() {
        init_common();
        mux = std::make_unique<AsyncMux>(tiers, metadata, placement, pool, true, 1);
    }
};

task<void> test_fs_types_are_expected(FixtureNoBg&) {
    const long hot_magic = fs_magic_for(kHotRoot);
    const long warm_magic = fs_magic_for(kWarmRoot);
    const long cold_magic = fs_magic_for(kColdRoot);

    assert_true(hot_magic == kTmpfsMagic, "hot tier must be tmpfs");

    const auto is_supported_disk_fs = [](long magic) {
        return magic == kExtMagic || magic == kXfsMagic;
    };

    assert_true(is_supported_disk_fs(warm_magic), "warm tier must be ext4 or xfs");
    assert_true(is_supported_disk_fs(cold_magic), "cold tier must be ext4 or xfs");

    assert_true(warm_magic != kTmpfsMagic, "warm tier must not be tmpfs");
    assert_true(cold_magic != kTmpfsMagic, "cold tier must not be tmpfs");

    co_return;
}

task<void> test_placement_targets_expected_tier(FixtureNoBg& fx) {
    fx.placement.set(1);
    auto hot = to_bytes("hot");
    co_await fx.mux->write("/hot_file", 0, span<const std::byte>(hot.data(), hot.size()));

    fx.placement.set(2);
    auto warm = to_bytes("warm");
    co_await fx.mux->write("/warm_file", 0, span<const std::byte>(warm.data(), warm.size()));

    fx.placement.set(3);
    auto cold = to_bytes("cold");
    co_await fx.mux->write("/cold_file", 0, span<const std::byte>(cold.data(), cold.size()));

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
    co_return;
}

task<void> test_multi_tier_fanout_read(FixtureNoBg& fx) {
    std::string payload((2 * kBlockSize) + 250, 'x');
    for (int i = 0; i < 26; ++i) {
        payload[100 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 100 + i] = static_cast<char>('a' + i);
        payload[(2 * kBlockSize) + 100 + (i % 10)] = static_cast<char>('0' + (i % 10));
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux->write("/fanout", 0, span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/fanout", 0, bytes.size());
    assert_true(locs.size() >= 3, "fanout payload should span at least three blocks");
    assert_non_overlapping(locs, "fanout");

    co_await fx.mux->migrate(locs[0].block_id, 1, 2);
    co_await fx.mux->migrate(locs[1].block_id, 1, 3);

    assert_true(fx.metadata.tier_of(locs[0].block_id) == 2,
                "first block should migrate to warm tier");
    assert_true(fx.metadata.tier_of(locs[1].block_id) == 3,
                "second block should migrate to cold tier");

    IoBuffer all = co_await fx.mux->read("/fanout", 0, bytes.size());
    assert_eq(to_string(all), payload,
              "full fanout read should assemble bytes from multiple tiers");

    IoBuffer partial = co_await fx.mux->read("/fanout", kBlockSize - 12, 40);
    assert_eq(to_string(partial), payload.substr(kBlockSize - 12, 40),
              "partial fanout read should match expected substring");
}

task<void> test_promote_restores_hot_tier(FixtureNoBg& fx) {
    std::string payload(10000, 'p');
    for (int i = 0; i < 26; ++i) {
        payload[200 + i] = static_cast<char>('A' + i);
        payload[7000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux->write("/promote", 0, span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/promote", 0, bytes.size());
    assert_true(!locs.empty(), "promote payload should create block metadata");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        const TierId dst = (i % 2 == 0) ? 2 : 3;
        co_await fx.mux->migrate(locs[i].block_id, 1, dst);
    }

    for (const auto& loc : locs) {
        co_await fx.mux->promote(loc.block_id, 1);
        assert_true(fx.metadata.tier_of(loc.block_id) == 1,
                    "promoted block should end on hot tier");
    }

    IoBuffer out = co_await fx.mux->read("/promote", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "promote should preserve bytes after moving blocks back to hot tier");
}

task<void> test_background_read_triggers_promotion(FixtureBg& fx) {
    std::string payload((2 * kBlockSize) + 321, 'm');
    for (int i = 0; i < 26; ++i) {
        payload[64 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 64 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux->write("/bg_promote", 0, span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/bg_promote", 0, bytes.size());
    assert_true(locs.size() >= 2, "bg_promote should span multiple blocks");

    for (const auto& loc : locs) {
        co_await fx.mux->migrate(loc.block_id, 1, 2);
        assert_true(fx.metadata.tier_of(loc.block_id) == 2,
                    "setup should move blocks to warm tier");
    }

    IoBuffer out = co_await fx.mux->read("/bg_promote", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "read should still return correct bytes before background promotion completes");

    const bool promoted = wait_until([&]() {
        auto now = sorted_locs(fx.metadata, "/bg_promote", 0, bytes.size());
        return std::all_of(now.begin(), now.end(), [](const BlockLocation& loc) {
            return loc.tier_id == 1;
        });
    });

    assert_true(promoted,
                "background promotion should eventually move read blocks back to hot tier");

    auto final_locs = sorted_locs(fx.metadata, "/bg_promote", 0, bytes.size());
    for (const auto& loc : final_locs) {
        assert_true(loc.tier_id == 1, "all blocks should be promoted to hot tier");
    }

    co_return;
}

task<void> test_multiple_paths_across_tiers(FixtureNoBg& fx) {
    fx.placement.set(1);
    auto one = to_bytes("one-hot");
    co_await fx.mux->write("/group/one", 0, span<const std::byte>(one.data(), one.size()));

    fx.placement.set(2);
    auto two = to_bytes("two-warm");
    co_await fx.mux->write("/group/two", 0, span<const std::byte>(two.data(), two.size()));

    fx.placement.set(3);
    auto three = to_bytes("three-cold");
    co_await fx.mux->write("/group/three", 0, span<const std::byte>(three.data(), three.size()));

    IoBuffer out_one = co_await fx.mux->read("/group/one", 0, one.size());
    IoBuffer out_two = co_await fx.mux->read("/group/two", 0, two.size());
    IoBuffer out_three = co_await fx.mux->read("/group/three", 0, three.size());

    assert_eq(to_string(out_one), "one-hot", "first path should stay correct");
    assert_eq(to_string(out_two), "two-warm", "second path should stay correct");
    assert_eq(to_string(out_three), "three-cold", "third path should stay correct");

    assert_true(fs::exists(kHotRoot / "group" / "one"), "hot root should contain /group/one");
    assert_true(fs::exists(kWarmRoot / "group" / "two"), "warm root should contain /group/two");
    assert_true(fs::exists(kColdRoot / "group" / "three"), "cold root should contain /group/three");

    fx.placement.set(1);
    co_return;
}

task<void> test_missing_file_zero_fill(FixtureNoBg& fx) {
    IoBuffer out = co_await fx.mux->read("/missing", 0, 20);
    assert_true(out.size() == 20, "missing file read should return requested length");
    for (std::byte b : out.data) {
        assert_true(b == std::byte{0}, "missing file read should be zero-filled");
    }
    co_return;
}

task<void> test_overwrite_replaces_old_extents(FixtureNoBg& fx) {
    fx.placement.set(1);
    const std::string base((2 * kBlockSize) + 64, 'A');
    auto base_bytes = to_bytes(base);
    co_await fx.mux->write("/overwrite", 0,
                          span<const std::byte>(base_bytes.data(), base_bytes.size()));

    fx.placement.set(2);
    const std::string patch(700, 'B');
    auto patch_bytes = to_bytes(patch);
    const std::uint64_t patch_off = kBlockSize - 150;
    co_await fx.mux->write("/overwrite", patch_off,
                          span<const std::byte>(patch_bytes.data(), patch_bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/overwrite", 0, base.size());
    assert_true(!locs.empty(), "overwrite should retain metadata");
    assert_non_overlapping(locs, "overwrite");

    for (const auto& loc : locs) {
        const bool overlaps_patch = !(loc.file_offset + loc.size <= patch_off ||
                                      loc.file_offset >= patch_off + patch.size());
        if (overlaps_patch) {
            assert_true(loc.tier_id == 2,
                        "patched extents should now point at the destination tier of the overwrite");
        }
    }

    std::string expected = base;
    expected.replace(static_cast<std::size_t>(patch_off), patch.size(), patch);

    IoBuffer out = co_await fx.mux->read("/overwrite", 0, expected.size());
    assert_eq(to_string(out), expected,
              "overwrite should replace old bytes instead of leaving stale overlaps");

    co_return;
}

task<void> test_cross_tier_overwrite_and_followup_read(FixtureNoBg& fx) {
    fx.placement.set(1);
    std::string payload((3 * kBlockSize) + 32, 'q');
    for (int i = 0; i < 26; ++i) {
        payload[50 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 50 + i] = static_cast<char>('a' + i);
        payload[(2 * kBlockSize) + 5 + i] = static_cast<char>('0' + (i % 10));
    }
    auto payload_bytes = to_bytes(payload);
    co_await fx.mux->write("/cross_overwrite", 0,
                          span<const std::byte>(payload_bytes.data(), payload_bytes.size()));

    auto before = sorted_locs(fx.metadata, "/cross_overwrite", 0, payload_bytes.size());
    assert_true(before.size() >= 3, "cross_overwrite should start with several extents");

    co_await fx.mux->migrate(before[0].block_id, 1, 2);
    co_await fx.mux->migrate(before[1].block_id, 1, 3);

    fx.placement.set(2);
    const std::string patch(kBlockSize + 333, 'Z');
    auto patch_bytes = to_bytes(patch);
    const std::uint64_t patch_off = kBlockSize - 100;
    co_await fx.mux->write("/cross_overwrite", patch_off,
                          span<const std::byte>(patch_bytes.data(), patch_bytes.size()));

    auto after = sorted_locs(fx.metadata, "/cross_overwrite", 0, payload.size());
    assert_non_overlapping(after, "cross_overwrite");

    payload.replace(static_cast<std::size_t>(patch_off), patch.size(), patch);

    IoBuffer out = co_await fx.mux->read("/cross_overwrite", 0, payload.size());
    assert_eq(to_string(out), payload,
              "read after cross-tier overwrite should reflect newest bytes across all tiers");

    co_return;
}

task<void> run_without_background(FixtureNoBg& fx) {
    co_await test_fs_types_are_expected(fx);
    co_await test_placement_targets_expected_tier(fx);
    co_await test_multi_tier_fanout_read(fx);
    co_await test_promote_restores_hot_tier(fx);
    co_await test_multiple_paths_across_tiers(fx);
    co_await test_missing_file_zero_fill(fx);
    co_await test_overwrite_replaces_old_extents(fx);
    co_await test_cross_tier_overwrite_and_followup_read(fx);
}

task<void> run_background_only(FixtureBg& fx) {
    co_await test_background_read_triggers_promotion(fx);
}

} // namespace

int main() {
    try {
        std::cerr << "starting no-bg tests\n";
        {
            FixtureNoBg fx;
            sync_wait(run_without_background(fx));
        }
        std::cerr << "finished no-bg tests\n";

        std::cerr << "starting bg tests\n";
        {
            FixtureBg fx;
            sync_wait(run_background_only(fx));
        }
        std::cerr << "finished bg tests\n";

        std::cout << kColorGreen
                  << "multiple_fs correctness tests passed."
                  << kColorReset << "\n";
        return 0;
    } catch (const std::exception& ex) {
        log_uncaught_exception(ex, __FILE__, __LINE__, __func__);
        return 1;
    } catch (...) {
        log_uncaught_nonstd(__FILE__, __LINE__, __func__);
        return 1;
    }
}