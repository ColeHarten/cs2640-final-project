#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "amux/asyncmux.hh"
#include "amux/tiers/filesystem_tier.hh"
#include "tests/utils.hh"

#include <cppcoro/sync_wait.hpp>

using asyncmux::AsyncMux;
using asyncmux::BlockLocation;
using asyncmux::Byte;
using asyncmux::ConstantPlacementPolicy;
using asyncmux::FileSystemTier;
using asyncmux::IoBuffer;
using asyncmux::MetadataStore;
using asyncmux::TierRegistry;
using asyncmux::kBlockSize;

using cppcoro::static_thread_pool;
using cppcoro::sync_wait;
using cppcoro::task;

namespace fs = std::filesystem;

namespace {

const fs::path kSingleRoot = "/tmp/asyncmux_single_fs";

void reset_dir(const fs::path& root) {
    std::error_code ec;
    fs::remove_all(root, ec);
    fs::create_directories(root, ec);
    if (ec) {
        throw std::runtime_error("failed to reset test directory");
    }
}

std::vector<BlockLocation> sorted_locs(MetadataStore& metadata,
                                       const std::string& path,
                                       std::uint64_t offset,
                                       std::uint64_t size) {
    auto locs = metadata.lookup(path, offset, size);
    std::sort(locs.begin(), locs.end(),
              [](const BlockLocation& a, const BlockLocation& b) {
                  if (a.file_offset != b.file_offset) {
                      return a.file_offset < b.file_offset;
                  }
                  return a.block_id < b.block_id;
              });
    return locs;
}

void assert_non_overlapping(const std::vector<BlockLocation>& locs,
                            const std::string& context) {
    for (std::size_t i = 1; i < locs.size(); ++i) {
        const auto prev_end = locs[i - 1].file_offset + locs[i - 1].size;
        assert_true(prev_end <= locs[i].file_offset,
                    context + ": metadata extents must be non-overlapping");
    }
}

struct Fixture {
    static_thread_pool pool{4};
    TierRegistry tiers;
    MetadataStore metadata;
    ConstantPlacementPolicy placement{1};
    AsyncMux mux;

    Fixture()
        : mux(tiers, metadata, placement, pool, false) {
        reset_dir(kSingleRoot);
        tiers.add(std::make_unique<FileSystemTier>(1, "single", kSingleRoot, pool));
    }
};

task<void> test_single_tier_round_trip(Fixture& fx) {
    auto bytes = to_bytes("hello asyncmux");
    co_await fx.mux.write("/alpha", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/alpha", 0, bytes.size());
    assert_eq(to_string(out), "hello asyncmux",
              "single tier write/read round-trip");

    auto locs = sorted_locs(fx.metadata, "/alpha", 0, bytes.size());
    assert_true(locs.size() == 1, "small write should create one extent");
    assert_true(locs[0].tier_id == 1, "single filesystem tier id should be 1");
    assert_non_overlapping(locs, "alpha");
    assert_true(fs::exists(kSingleRoot / "alpha"), "file should exist under root");
}

task<void> test_partial_and_cross_block_read(Fixture& fx) {
    std::string payload(kBlockSize + 16, 'q');
    payload[kBlockSize - 2] = 'X';
    payload[kBlockSize - 1] = 'Y';
    payload[kBlockSize] = 'Z';
    payload[kBlockSize + 1] = 'W';

    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/beta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer partial = co_await fx.mux.read("/beta", 3, 5);
    assert_eq(to_string(partial), payload.substr(3, 5),
              "partial read should return requested bytes");

    IoBuffer cross = co_await fx.mux.read("/beta", kBlockSize - 2, 4);
    assert_eq(to_string(cross), "XYZW",
              "cross-block read should stitch bytes correctly");

    auto locs = sorted_locs(fx.metadata, "/beta", 0, bytes.size());
    assert_true(locs.size() == 2, "beta should span exactly two extents");
    assert_non_overlapping(locs, "beta");
}

task<void> test_sparse_write_zero_fill(Fixture& fx) {
    auto bytes = to_bytes("tail");
    const std::uint64_t write_offset = 10;

    co_await fx.mux.write("/gamma", write_offset,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/gamma", 0, write_offset + bytes.size());

    std::string expected(write_offset, '\0');
    expected += "tail";
    assert_eq(to_string(out), expected,
              "sparse write should read back zero-filled gap then data");
}

task<void> test_missing_file_reads_as_zeroes(Fixture& fx) {
    IoBuffer out = co_await fx.mux.read("/does-not-exist", 0, 32);
    assert_true(out.size() == 32, "missing file read should honor requested size");
    for (Byte b : out.data) {
        assert_true(b == Byte{0}, "missing file bytes should be zero-filled");
    }
}

task<void> test_two_files_are_isolated(Fixture& fx) {
    auto left = to_bytes("left-side");
    auto right = to_bytes("right-side");

    co_await fx.mux.write("/dir/a", 0,
                          std::span<const Byte>(left.data(), left.size()));
    co_await fx.mux.write("/dir/b", 0,
                          std::span<const Byte>(right.data(), right.size()));

    IoBuffer out_left = co_await fx.mux.read("/dir/a", 0, left.size());
    IoBuffer out_right = co_await fx.mux.read("/dir/b", 0, right.size());

    assert_eq(to_string(out_left), "left-side", "first file should keep its bytes");
    assert_eq(to_string(out_right), "right-side", "second file should keep its bytes");

    assert_true(fs::exists(kSingleRoot / "dir" / "a"), "first path should exist on disk");
    assert_true(fs::exists(kSingleRoot / "dir" / "b"), "second path should exist on disk");
}

task<void> test_overwrite_replaces_old_data(Fixture& fx) {
    auto first = to_bytes("abcdefghij");
    co_await fx.mux.write("/overwrite", 0,
                          std::span<const Byte>(first.data(), first.size()));

    auto second = to_bytes("XYZ");
    co_await fx.mux.write("/overwrite", 3,
                          std::span<const Byte>(second.data(), second.size()));

    IoBuffer out = co_await fx.mux.read("/overwrite", 0, 10);
    assert_eq(to_string(out), "abcXYZghij",
              "overwrite should replace old bytes, not duplicate extents");

    auto locs = sorted_locs(fx.metadata, "/overwrite", 0, 10);
    assert_non_overlapping(locs, "overwrite");
}

task<void> test_cross_block_overwrite_replaces_old_data(Fixture& fx) {
    std::string payload(kBlockSize + 32, 'a');
    payload[kBlockSize - 1] = 'L';
    payload[kBlockSize] = 'R';

    auto original = to_bytes(payload);
    co_await fx.mux.write("/cross_overwrite", 0,
                          std::span<const Byte>(original.data(), original.size()));

    std::string patch = "0123456789";
    const std::uint64_t patch_offset = kBlockSize - 4;
    auto patch_bytes = to_bytes(patch);
    co_await fx.mux.write("/cross_overwrite", patch_offset,
                          std::span<const Byte>(patch_bytes.data(), patch_bytes.size()));

    payload.replace(static_cast<std::size_t>(patch_offset), patch.size(), patch);

    IoBuffer out = co_await fx.mux.read("/cross_overwrite", 0, payload.size());
    assert_eq(to_string(out), payload,
              "cross-block overwrite should preserve newest bytes across extent boundaries");

    IoBuffer window = co_await fx.mux.read("/cross_overwrite", patch_offset, patch.size());
    assert_eq(to_string(window), patch,
              "window around cross-block overwrite should match patch");

    auto locs = sorted_locs(fx.metadata, "/cross_overwrite", 0, payload.size());
    assert_non_overlapping(locs, "cross_overwrite");
}

task<void> test_promote_noop_on_single_tier(Fixture& fx) {
    std::string payload(7000, 'p');
    for (int i = 0; i < 26; ++i) {
        payload[100 + i] = static_cast<char>('A' + i);
    }

    auto bytes = to_bytes(payload);
    co_await fx.mux.write("/delta", 0,
                          std::span<const Byte>(bytes.data(), bytes.size()));

    auto locs = sorted_locs(fx.metadata, "/delta", 0, bytes.size());
    assert_true(!locs.empty(), "delta should create at least one block");

    for (const auto& loc : locs) {
        co_await fx.mux.promote(loc.block_id, 1);
        assert_true(fx.metadata.tier_of(loc.block_id) == 1,
                    "promote should keep block on tier 1");
    }

    IoBuffer out = co_await fx.mux.read("/delta", 0, bytes.size());
    assert_eq(to_string(out), payload,
              "promote noop should preserve data in single-tier mode");
}

task<void> run_all(Fixture& fx) {
    co_await test_single_tier_round_trip(fx);
    co_await test_partial_and_cross_block_read(fx);
    co_await test_sparse_write_zero_fill(fx);
    co_await test_missing_file_reads_as_zeroes(fx);
    co_await test_two_files_are_isolated(fx);
    co_await test_overwrite_replaces_old_data(fx);
    co_await test_cross_block_overwrite_replaces_old_data(fx);
    co_await test_promote_noop_on_single_tier(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        sync_wait(run_all(fx));
        std::cout << kColorGreen
                  << "single_fs correctness tests passed."
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