#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "amux/asyncmux.hh"
#include "tests/utils.hh"

#include <cppcoro/sync_wait.hpp>

/*
 * Correctness-focused integration tests for AsyncMux configured with multiple
 * filesystem-backed tiers.
 *
 * This suite uses three SegmentFileTier instances rooted at local directories:
 *   tier 1 -> /tmp/asyncmux_test_hot
 *   tier 2 -> /tmp/asyncmux_test_warm
 *   tier 3 -> /tmp/asyncmux_test_cold
 *
 * Intended coverage:
 * - byte-accurate read/write behavior
 * - partial and cross-block reads
 * - multi-block fan-out reads after blocks are migrated to different tiers
 * - metadata updates during migration/promotion
 * - segment-file placement under the expected tier roots
 * - current scaffold behavior for missing-file reads and overlapping writes
 *
 * Auto-migration is disabled so tests can explicitly control placement.
 */

using asyncmux::AsyncMux;
using asyncmux::BlockId;
using asyncmux::BlockLocation;
using asyncmux::Byte;
using asyncmux::SegmentFileTier;
using asyncmux::IoBuffer;
using asyncmux::MetadataStore;
using asyncmux::PlacementPolicy;
using asyncmux::TierId;
using asyncmux::TierRegistry;
using asyncmux::WriteBlock;

using cppcoro::static_thread_pool;
using cppcoro::sync_wait;
using cppcoro::task;

namespace fs = std::filesystem;

namespace {

constexpr const char* kHotRoot  = "/tmp/asyncmux_test_hot";
constexpr const char* kWarmRoot = "/tmp/asyncmux_test_warm";
constexpr const char* kColdRoot = "/tmp/asyncmux_test_cold";

fs::path segment_path(const fs::path& root, std::uint64_t segment_id) {
    return root / ("segment_" + std::to_string(segment_id) + ".bin");
}

void reset_dir(const fs::path& root) {
    std::error_code ec;
    fs::remove_all(root, ec);
    fs::create_directories(root);
}

void reset_all_tier_dirs() {
    reset_dir(kHotRoot);
    reset_dir(kWarmRoot);
    reset_dir(kColdRoot);
}

/*
 * A test-only placement policy that can be changed at runtime.
 * By default, all new writes go to tier 1.
 */
class MutablePlacementPolicy final : public PlacementPolicy {
public:
    explicit MutablePlacementPolicy(TierId tier) : current_tier_(tier) {}

    void set_tier(TierId tier) {
        current_tier_ = tier;
    }

    TierId choose_tier(const WriteBlock&) override {
        return current_tier_;
    }

private:
    TierId current_tier_;
};

struct Fixture {
    static_thread_pool pool{4};
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};
    AsyncMux mux;

    Fixture()
        : mux(tiers, metadata, placement, pool, false) {
        reset_all_tier_dirs();

        tiers.add(std::make_unique<SegmentFileTier>(1, "hot",  kHotRoot,  pool));
        tiers.add(std::make_unique<SegmentFileTier>(2, "warm", kWarmRoot, pool));
        tiers.add(std::make_unique<SegmentFileTier>(3, "cold", kColdRoot, pool));
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

task<void> test_basic_write_then_read(Fixture& fx) {
    auto bytes = to_bytes("hello asyncmux");
    co_await fx.mux.write("/alpha", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/alpha", 0, bytes.size());
    assert_eq(to_string(out), "hello asyncmux",
              "basic write/read round-trip on filesystem tiers");
}

task<void> test_partial_read(Fixture& fx) {
    auto bytes = to_bytes("abcdefghijklmnop");
    co_await fx.mux.write("/beta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/beta", 3, 5);
    assert_eq(to_string(out), "defgh",
              "partial read returns requested slice");
}

task<void> test_multi_block_round_trip(Fixture& fx) {
    std::string large(9000, 'x');
    for (int i = 0; i < 26; ++i) {
        large[300 + i] = static_cast<char>('A' + i);
        large[5000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/gamma", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/gamma", 0, bytes.size());
    assert_eq(to_string(out), large,
              "multi-block write/read round-trip");
}

task<void> test_cross_block_partial_read(Fixture& fx) {
    std::string large(5000, 'q');
    large[4094] = 'X';
    large[4095] = 'Y';
    large[4096] = 'Z';
    large[4097] = 'W';

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/delta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/delta", 4094, 4);
    assert_eq(to_string(out), "XYZW",
              "read spanning block boundary");
}

task<void> test_initial_write_creates_segment_files_in_hot_tier(Fixture& fx) {
    std::string large(9000, 'm');
    auto bytes = to_bytes(large);

    co_await fx.mux.write("/epsilon", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/epsilon", 0, bytes.size());
    assert_true(locs.size() >= 3,
                "9000-byte write should create at least three 4KB-ish blocks");

    for (const auto& loc : locs) {
        assert_true(loc.tier_id == 1,
                    "initial placement should put blocks on hot tier");
        assert_true(loc.size > 0,
                    "each mapping should have a positive byte size");
        assert_true(fs::exists(segment_path(kHotRoot, loc.segment_id)),
                    "segment file should exist under hot tier root");
    }
}

task<void> test_migrate_updates_metadata_and_moves_extent(Fixture& fx) {
    auto bytes = to_bytes("promote-me");
    co_await fx.mux.write("/zeta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/zeta", 0, bytes.size());
    BlockId block = locs.at(0).block_id;

    assert_true(fx.metadata.tier_of(block) == 1,
                "block starts on hot tier by default");
    const auto before = fx.metadata.get_block(block);
    assert_true(fs::exists(segment_path(kHotRoot, before.segment_id)),
                "segment file initially exists in hot tier");

    co_await fx.mux.migrate(block, 1, 2);

    assert_true(fx.metadata.tier_of(block) == 2,
                "migrate updates metadata to warm tier");
    const auto after = fx.metadata.get_block(block);
    assert_true(fs::exists(segment_path(kWarmRoot, after.segment_id)),
                "migrated extent appears in warm tier segment");
    assert_true((after.segment_id != before.segment_id) ||
                    (after.tier_offset != before.tier_offset),
                "migration should change physical placement metadata");

    IoBuffer out = co_await fx.mux.read("/zeta", 0, bytes.size());
    assert_eq(to_string(out), "promote-me",
              "data survives migration from hot to warm");
}

task<void> test_promote_chain_across_three_tiers(Fixture& fx) {
    std::string large(7000, 'p');
    for (int i = 0; i < 20; ++i) {
        large[100 + i] = static_cast<char>('A' + i);
    }

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/eta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/eta", 0, bytes.size());
    assert_true(locs.size() >= 2,
                "test file should span multiple blocks");

    BlockId first  = locs[0].block_id;
    BlockId second = locs[1].block_id;

    co_await fx.mux.migrate(first, 1, 2);
    co_await fx.mux.migrate(second, 1, 3);

    assert_true(fx.metadata.tier_of(first) == 2,
                "first block moved to warm tier");
    assert_true(fx.metadata.tier_of(second) == 3,
                "second block moved to cold tier");

    co_await fx.mux.promote(first, 1);
    co_await fx.mux.promote(second, 1);

    assert_true(fx.metadata.tier_of(first) == 1,
                "first block promoted back to hot tier");
    assert_true(fx.metadata.tier_of(second) == 1,
                "second block promoted back to hot tier");

    IoBuffer out = co_await fx.mux.read("/eta", 0, bytes.size());
    assert_eq(to_string(out), large,
              "data survives promote chain across three tiers");
}

task<void> test_fanout_read_from_multiple_tiers(Fixture& fx) {
    std::string large(13000, 'r');
    for (int i = 0; i < 26; ++i) {
        large[100 + i]   = static_cast<char>('A' + i);
        large[4500 + i]  = static_cast<char>('a' + i);
        large[9000 + i]  = static_cast<char>('0' + (i % 10));
    }

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/theta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/theta", 0, bytes.size());
    assert_true(locs.size() >= 4,
                "test file should span at least four blocks");

    // Move different blocks to different tiers so one read fans out.
    co_await fx.mux.migrate(locs[0].block_id, 1, 2);
    co_await fx.mux.migrate(locs[1].block_id, 1, 3);
    // leave later blocks on hot tier

    assert_true(fx.metadata.tier_of(locs[0].block_id) == 2,
                "first block on warm tier");
    assert_true(fx.metadata.tier_of(locs[1].block_id) == 3,
                "second block on cold tier");

    IoBuffer out = co_await fx.mux.read("/theta", 0, bytes.size());
    assert_eq(to_string(out), large,
              "full-file read assembles data correctly from multiple tiers");

    IoBuffer partial = co_await fx.mux.read("/theta", 4000, 6000);
    assert_eq(to_string(partial), large.substr(4000, 6000),
              "partial read assembles correctly across tier boundaries");
}

task<void> test_new_writes_can_target_nondefault_tiers(Fixture& fx) {
    fx.placement.set_tier(3);

    auto bytes = to_bytes("cold-start");
    co_await fx.mux.write("/iota", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/iota", 0, bytes.size());
    assert_true(locs.size() == 1,
                "small write should create one block");
    assert_true(locs[0].tier_id == 3,
                "placement policy can direct new writes to cold tier");
    assert_true(locs[0].size > 0,
                "single mapping should have valid size");
    assert_true(fs::exists(segment_path(kColdRoot, locs[0].segment_id)),
                "new segment file should be created under cold tier root");

    IoBuffer out = co_await fx.mux.read("/iota", 0, bytes.size());
    assert_eq(to_string(out), "cold-start",
              "data written directly to cold tier is readable");

    // Keep later tests deterministic when sharing one fixture instance.
    fx.placement.set_tier(1);
}

task<void> test_missing_file_read_returns_zero_fill(Fixture& fx) {
    IoBuffer out = co_await fx.mux.read("/does-not-exist", 0, 32);
    assert_true(out.data.size() == 32,
                "current scaffold zero-fills missing file reads to requested size");
    for (Byte b : out.data) {
        assert_true(b == Byte{0},
                    "missing file read should currently return zeros");
    }
}

task<void> test_overwrite_appends_new_metadata_in_current_scaffold(Fixture& fx) {
    auto first  = to_bytes("AAAA");
    auto second = to_bytes("BBBB");

    co_await fx.mux.write("/kappa", 0,
                          std::span<const Byte>(first.data(), first.size()));
    co_await fx.mux.write("/kappa", 0,
                          std::span<const Byte>(second.data(), second.size()));

    auto locs = fx.metadata.lookup("/kappa", 0, 4);
    assert_true(locs.size() >= 2,
                "current scaffold records multiple versions on overlapping writes");
}

task<void> test_metadata_contains_physical_extent_fields(Fixture& fx) {
    std::string payload(9000, 's');
    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/mu", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/mu", 0, bytes.size());
    assert_true(!locs.empty(), "write should create metadata locations");

    for (const auto& loc : locs) {
        assert_true(loc.tier_offset < (64ull * 1024 * 1024),
                    "tier offset should be initialized in segment range");
        assert_true(loc.size > 0,
                    "extent size should be positive");

        fs::path root;
        if (loc.tier_id == 1) {
            root = kHotRoot;
        } else if (loc.tier_id == 2) {
            root = kWarmRoot;
        } else {
            root = kColdRoot;
        }
        assert_true(fs::exists(segment_path(root, loc.segment_id)),
                    "segment id should point to an existing tier segment file");
    }
}

task<void> test_migration_updates_tier_and_extent_metadata(Fixture& fx) {
    auto bytes = to_bytes("metadata-move");
    co_await fx.mux.write("/nu", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/nu", 0, bytes.size());
    assert_true(!locs.empty(), "nu write should create one location");
    const BlockId block = locs[0].block_id;
    const auto before = fx.metadata.get_block(block);

    co_await fx.mux.migrate(block, before.tier_id, 2);
    const auto after = fx.metadata.get_block(block);

    assert_true(after.tier_id == 2,
                "tier id should change after migration");
    assert_true((after.segment_id != before.segment_id) ||
                    (after.tier_offset != before.tier_offset),
                "physical extent metadata should change after migration");

    IoBuffer out = co_await fx.mux.read("/nu", 0, bytes.size());
    assert_eq(to_string(out), "metadata-move",
              "bytes remain correct after migration metadata update");
}

task<void> test_migration_round_trip_preserves_bytes(Fixture& fx) {
    fx.placement.set_tier(1);

    std::string payload(10000, 'z');
    for (int i = 0; i < 26; ++i) {
        payload[1000 + i] = static_cast<char>('A' + i);
        payload[7000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/lambda", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/lambda", 0, bytes.size());
    assert_true(!locs.empty(), "lambda file should have block locations");

    for (std::size_t i = 0; i < locs.size(); ++i) {
        TierId src = fx.metadata.tier_of(locs[i].block_id);
        TierId dst = (i % 2 == 0) ? 2 : 3;
        if (src != dst) {
            co_await fx.mux.migrate(locs[i].block_id, src, dst);
        }
    }

    IoBuffer after_demote = co_await fx.mux.read("/lambda", 0, bytes.size());
    assert_eq(to_string(after_demote), payload,
              "data remains correct after spreading blocks across warm/cold tiers");

    for (const auto& loc : locs) {
        TierId src = fx.metadata.tier_of(loc.block_id);
        if (src != 1) {
            co_await fx.mux.promote(loc.block_id, 1);
        }
    }

    IoBuffer after_promote = co_await fx.mux.read("/lambda", 0, bytes.size());
    assert_eq(to_string(after_promote), payload,
              "data remains correct after promoting all blocks back to hot tier");
}

task<void> run_all_tests(Fixture& fx) {
    co_await test_basic_write_then_read(fx);
    co_await test_partial_read(fx);
    co_await test_multi_block_round_trip(fx);
    co_await test_cross_block_partial_read(fx);
    co_await test_initial_write_creates_segment_files_in_hot_tier(fx);
    co_await test_migrate_updates_metadata_and_moves_extent(fx);
    co_await test_promote_chain_across_three_tiers(fx);
    co_await test_fanout_read_from_multiple_tiers(fx);
    co_await test_new_writes_can_target_nondefault_tiers(fx);
    co_await test_missing_file_read_returns_zero_fill(fx);
    co_await test_overwrite_appends_new_metadata_in_current_scaffold(fx);
    co_await test_metadata_contains_physical_extent_fields(fx);
    co_await test_migration_updates_tier_and_extent_metadata(fx);
    co_await test_migration_round_trip_preserves_bytes(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        sync_wait(run_all_tests(fx));
        std::cout << kColorGreen
                  << "Single filesytem multi-tier tests passed."
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