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
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "../bmux/blockmux.hh"
#include "tests/utils.hh"

using bmux::BlockingMux;
using bmux::BlockLocation;
using bmux::FileSystemTier;
using bmux::IoBuffer;
using bmux::MetadataStore;
using bmux::MutablePlacementPolicy;
using bmux::ThreadPool;
using bmux::TierId;
using bmux::TierRegistry;
using bmux::kBlockSize;

namespace {

static_assert(
#ifdef __linux__
    true,
#else
    false,
#endif
    "corr_block.cc requires Linux");

std::string normalize_test_path(const std::string& path) {
    fs::path p(path);
    p = p.lexically_normal();
    if (p.is_absolute()) {
        p = p.lexically_relative("/");
    }
    return p.string();
}

std::vector<BlockLocation> sorted_locs_blocking(MetadataStore& metadata,
                                                const std::string& path,
                                                std::uint64_t offset,
                                                std::uint64_t size) {
    auto locs = metadata.lookup(normalize_test_path(path), offset, size);
    std::sort(locs.begin(), locs.end(),
              [](const BlockLocation& a, const BlockLocation& b) {
                  return a.file_offset < b.file_offset;
              });
    return locs;
}

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

struct Fixture {
    ThreadPool pool{8}; // optional standalone pool if you want it available
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};
    BlockingMux mux;

    Fixture()
        : mux(tiers, metadata, placement, 8, true, 1) {
        wait_for_mount(kHotRoot, "hot");
        wait_for_mount(kWarmRoot, "warm");
        wait_for_mount(kColdRoot, "cold");

        ensure_tier_ready(kHotRoot, "hot");
        ensure_tier_ready(kWarmRoot, "warm");
        ensure_tier_ready(kColdRoot, "cold");

        cleanup_test_artifacts();

        tiers.add(std::make_unique<FileSystemTier>(1, "hot", kHotRoot));
        tiers.add(std::make_unique<FileSystemTier>(2, "warm", kWarmRoot));
        tiers.add(std::make_unique<FileSystemTier>(3, "cold", kColdRoot));
    }

    ~Fixture() {
        cleanup_test_artifacts();
    }
};

void test_fs_types_are_expected(Fixture&) {
    const long hot_magic = fs_magic_for(kHotRoot);
    const long warm_magic = fs_magic_for(kWarmRoot);
    const long cold_magic = fs_magic_for(kColdRoot);

    assert_true(hot_magic == kTmpfsMagic,
                "hot tier must be tmpfs");

    const auto is_supported_disk_fs = [](long magic) {
        return magic == kExtMagic || magic == kXfsMagic;
    };

    assert_true(is_supported_disk_fs(warm_magic),
                "warm tier must be ext4 or xfs");
    assert_true(is_supported_disk_fs(cold_magic),
                "cold tier must be ext4 or xfs");

    assert_true(warm_magic != kTmpfsMagic,
                "warm tier must not be tmpfs");
    assert_true(cold_magic != kTmpfsMagic,
                "cold tier must not be tmpfs");
}

void test_placement_targets_expected_tier(Fixture& fx) {
    fx.placement.set(1);
    auto hot = to_bytes("hot");
    fx.mux.write("/hot_file", 0,
                 span<const std::byte>(hot.data(), hot.size()));

    fx.placement.set(2);
    auto warm = to_bytes("warm");
    fx.mux.write("/warm_file", 0,
                 span<const std::byte>(warm.data(), warm.size()));

    fx.placement.set(3);
    auto cold = to_bytes("cold");
    fx.mux.write("/cold_file", 0,
                 span<const std::byte>(cold.data(), cold.size()));

    const auto hot_locs = sorted_locs_blocking(fx.metadata, "/hot_file", 0, hot.size());
    const auto warm_locs = sorted_locs_blocking(fx.metadata, "/warm_file", 0, warm.size());
    const auto cold_locs = sorted_locs_blocking(fx.metadata, "/cold_file", 0, cold.size());

    assert_true(!hot_locs.empty(), "hot_file should have metadata");
    assert_true(!warm_locs.empty(), "warm_file should have metadata");
    assert_true(!cold_locs.empty(), "cold_file should have metadata");

    const auto hot_loc = hot_locs.at(0);
    const auto warm_loc = warm_locs.at(0);
    const auto cold_loc = cold_locs.at(0);

    assert_true(hot_loc.tier_id == 1, "hot write should land on tier 1");
    assert_true(warm_loc.tier_id == 2, "warm write should land on tier 2");
    assert_true(cold_loc.tier_id == 3, "cold write should land on tier 3");

    assert_true(fs::exists(kHotRoot / "hot_file"), "hot root should contain hot_file");
    assert_true(fs::exists(kWarmRoot / "warm_file"), "warm root should contain warm_file");
    assert_true(fs::exists(kColdRoot / "cold_file"), "cold root should contain cold_file");

    fx.placement.set(1);
}

void test_multi_tier_fanout_read(Fixture& fx) {
    std::string payload((2 * kBlockSize) + 250, 'x');
    for (int i = 0; i < 26; ++i) {
        payload[100 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 100 + i] = static_cast<char>('a' + i);
        payload[(2 * kBlockSize) + 100 + (i % 10)] = static_cast<char>('0' + (i % 10));
    }

    auto bytes = to_bytes(payload);
    fx.mux.write("/fanout", 0,
                 span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs_blocking(fx.metadata, "/fanout", 0, bytes.size());
    assert_true(locs.size() >= 3, "fanout payload should span at least three blocks");
    assert_non_overlapping(locs, "fanout");

    fx.mux.migrate(locs[0].block_id, 1, 2);
    fx.mux.migrate(locs[1].block_id, 1, 3);

    assert_true(fx.metadata.tier_of(locs[0].block_id) == 2,
                "first block should migrate to warm tier");
    assert_true(fx.metadata.tier_of(locs[1].block_id) == 3,
                "second block should migrate to cold tier");

    IoBuffer all = fx.mux.read("/fanout", 0, bytes.size());
    assert_eq(to_string(all), payload,
              "full fanout read should assemble bytes from multiple tiers");

    IoBuffer partial = fx.mux.read("/fanout", kBlockSize - 12, 40);
    assert_eq(to_string(partial), payload.substr(kBlockSize - 12, 40),
              "partial fanout read should match expected substring");
}

void test_promote_restores_hot_tier(Fixture& fx) {
    std::string payload(10000, 'p');
    for (int i = 0; i < 26; ++i) {
        payload[200 + i] = static_cast<char>('A' + i);
        payload[7000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    fx.mux.write("/promote", 0,
                 span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs_blocking(fx.metadata, "/promote", 0, bytes.size());
    assert_true(!locs.empty(), "promote payload should create block metadata");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        const TierId dst = (i % 2 == 0) ? 2 : 3;
        fx.mux.migrate(locs[i].block_id, 1, dst);
    }

    for (const auto& loc : locs) {
        fx.mux.promote(loc.block_id, 1);
        assert_true(fx.metadata.tier_of(loc.block_id) == 1,
                    "promoted block should end on hot tier");
    }

    IoBuffer out = fx.mux.read("/promote", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "promote should preserve bytes after moving blocks back to hot tier");
}

void test_background_read_triggers_promotion(Fixture& fx) {
    std::string payload((2 * kBlockSize) + 321, 'm');
    for (int i = 0; i < 26; ++i) {
        payload[64 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 64 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    fx.mux.write("/bg_promote", 0,
                 span<const std::byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs_blocking(fx.metadata, "/bg_promote", 0, bytes.size());
    assert_true(locs.size() >= 2, "bg_promote should span multiple blocks");

    for (const auto& loc : locs) {
        fx.mux.migrate(loc.block_id, 1, 2);
        assert_true(fx.metadata.tier_of(loc.block_id) == 2,
                    "setup should move blocks to warm tier");
    }

    IoBuffer out = fx.mux.read("/bg_promote", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "read should still return correct bytes before background promotion completes");

    fx.mux.wait_for_background_idle();

    auto final_locs = sorted_locs_blocking(fx.metadata, "/bg_promote", 0, bytes.size());
    for (const auto& loc : final_locs) {
        assert_true(loc.tier_id == 1, "all blocks should be promoted to hot tier");
    }
}

void test_multiple_paths_across_tiers(Fixture& fx) {
    fx.placement.set(1);
    auto one = to_bytes("one-hot");
    fx.mux.write("/group/one", 0,
                 span<const std::byte>(one.data(), one.size()));

    fx.placement.set(2);
    auto two = to_bytes("two-warm");
    fx.mux.write("/group/two", 0,
                 span<const std::byte>(two.data(), two.size()));

    fx.placement.set(3);
    auto three = to_bytes("three-cold");
    fx.mux.write("/group/three", 0,
                 span<const std::byte>(three.data(), three.size()));

    IoBuffer out_one = fx.mux.read("/group/one", 0, one.size());
    IoBuffer out_two = fx.mux.read("/group/two", 0, two.size());
    IoBuffer out_three = fx.mux.read("/group/three", 0, three.size());

    assert_eq(to_string(out_one), "one-hot", "first path should stay correct");
    assert_eq(to_string(out_two), "two-warm", "second path should stay correct");
    assert_eq(to_string(out_three), "three-cold", "third path should stay correct");

    assert_true(fs::exists(kHotRoot / "group" / "one"), "hot root should contain /group/one");
    assert_true(fs::exists(kWarmRoot / "group" / "two"), "warm root should contain /group/two");
    assert_true(fs::exists(kColdRoot / "group" / "three"), "cold root should contain /group/three");

    fx.placement.set(1);
}

void test_missing_file_zero_fill(Fixture& fx) {
    IoBuffer out = fx.mux.read("/missing", 0, 20);
    assert_true(out.size() == 20, "missing file read should return requested length");
    for (std::byte b : out.data) {
        assert_true(b == std::byte{0}, "missing file read should be zero-filled");
    }
}

void test_overwrite_replaces_old_extents(Fixture& fx) {
    fx.placement.set(1);
    const std::string base((2 * kBlockSize) + 64, 'A');
    auto base_bytes = to_bytes(base);
    fx.mux.write("/overwrite", 0,
                 span<const std::byte>(base_bytes.data(), base_bytes.size()));

    fx.placement.set(2);
    const std::string patch(700, 'B');
    auto patch_bytes = to_bytes(patch);
    const std::uint64_t patch_off = kBlockSize - 150;
    fx.mux.write("/overwrite", patch_off,
                 span<const std::byte>(patch_bytes.data(), patch_bytes.size()));

    std::string expected = base;
    expected.replace(static_cast<std::size_t>(patch_off), patch.size(), patch);

    auto locs = sorted_locs_blocking(fx.metadata, "/overwrite", 0, expected.size());
    assert_true(!locs.empty(), "overwrite should retain metadata");
    assert_non_overlapping(locs, "overwrite");

    std::uint64_t covered = 0;
    for (const auto& loc : locs) {
        const std::uint64_t loc_begin = loc.file_offset;
        const std::uint64_t loc_end = loc.file_offset + loc.size;
        const std::uint64_t patch_begin = patch_off;
        const std::uint64_t patch_end = patch_off + patch.size();

        const std::uint64_t overlap_begin = std::max(loc_begin, patch_begin);
        const std::uint64_t overlap_end = std::min(loc_end, patch_end);

        if (overlap_begin < overlap_end && loc.tier_id == 2) {
            covered += (overlap_end - overlap_begin);
        }
    }

    assert_true(covered == patch.size(),
                "patched byte range should be fully covered by tier-2 extents");

    IoBuffer out = fx.mux.read("/overwrite", 0, expected.size());
    assert_eq(to_string(out), expected,
              "overwrite should replace old bytes instead of leaving stale overlaps");
}

void test_cross_tier_overwrite_and_followup_read(Fixture& fx) {
    fx.placement.set(1);
    std::string payload((3 * kBlockSize) + 32, 'q');
    for (int i = 0; i < 26; ++i) {
        payload[50 + i] = static_cast<char>('A' + i);
        payload[kBlockSize + 50 + i] = static_cast<char>('a' + i);
        payload[(2 * kBlockSize) + 5 + i] = static_cast<char>('0' + (i % 10));
    }

    auto payload_bytes = to_bytes(payload);
    fx.mux.write("/cross_overwrite", 0,
                 span<const std::byte>(payload_bytes.data(), payload_bytes.size()));

    auto before = sorted_locs_blocking(fx.metadata, "/cross_overwrite", 0, payload_bytes.size());
    assert_true(before.size() >= 3, "cross_overwrite should start with several extents");

    fx.mux.migrate(before[0].block_id, 1, 2);
    fx.mux.migrate(before[1].block_id, 1, 3);

    fx.placement.set(2);
    const std::string patch(kBlockSize + 333, 'Z');
    auto patch_bytes = to_bytes(patch);
    const std::uint64_t patch_off = kBlockSize - 100;
    fx.mux.write("/cross_overwrite", patch_off,
                 span<const std::byte>(patch_bytes.data(), patch_bytes.size()));

    payload.replace(static_cast<std::size_t>(patch_off), patch.size(), patch);

    IoBuffer out = fx.mux.read("/cross_overwrite", 0, payload.size());
    assert_eq(to_string(out), payload,
              "read after cross-tier overwrite should reflect newest bytes across all tiers");

    auto after = sorted_locs_blocking(fx.metadata, "/cross_overwrite", 0, payload.size());
    assert_non_overlapping(after, "cross_overwrite");
}

void run_all(Fixture& fx) {
    test_fs_types_are_expected(fx);
    test_placement_targets_expected_tier(fx);
    test_multi_tier_fanout_read(fx);
    test_promote_restores_hot_tier(fx);
    test_background_read_triggers_promotion(fx);
    test_multiple_paths_across_tiers(fx);
    test_missing_file_zero_fill(fx);
    test_overwrite_replaces_old_extents(fx);
    test_cross_tier_overwrite_and_followup_read(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        run_all(fx);
        std::cout << kColorGreen
                  << "multiple_fs_blocking correctness tests passed."
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