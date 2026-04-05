#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "amux/asyncmux.hh"
#include <cppcoro/sync_wait.hpp>

using asyncmux::AsyncMux;
using asyncmux::BlockId;
using asyncmux::Byte;
using asyncmux::IoBuffer;
using asyncmux::MemoryTier;
using asyncmux::MetadataStore;
using asyncmux::SimplePlacementPolicy;
using asyncmux::TierRegistry;

namespace {

constexpr const char* kColorRed = "\x1b[31m";
constexpr const char* kColorBoldGreen = "\x1b[1;32m";
constexpr const char* kColorReset = "\x1b[0m";

std::vector<Byte> to_bytes(const std::string& s) {
    std::vector<Byte> out;
    out.reserve(s.size());
    for (unsigned char ch : s) {
        out.push_back(static_cast<Byte>(ch));
    }
    return out;
}

std::string to_string(const IoBuffer& buf) {
    std::string out;
    out.reserve(buf.data.size());
    for (Byte b : buf.data) {
        out.push_back(static_cast<char>(b));
    }
    return out;
}

void assert_eq(const std::string& actual,
               const std::string& expected,
               const std::string& message) {
    if (actual != expected) {
        std::cerr << kColorRed << "FAIL:" << kColorReset << " " << message << "\n"
                  << "  expected: [" << expected << "]\n"
                  << "  actual:   [" << actual << "]\n";
        std::abort();
    }
}

void assert_true(bool cond, const std::string& message) {
    if (!cond) {
        std::cerr << kColorRed << "FAIL:" << kColorReset << " " << message << "\n";
        std::abort();
    }
}

struct Fixture {
    cppcoro::static_thread_pool pool{4};
    TierRegistry tiers;
    MetadataStore metadata;
    SimplePlacementPolicy placement{1};
    AsyncMux mux;

    Fixture()
        : mux(tiers, metadata, placement, pool) {
        tiers.add(std::make_unique<MemoryTier>(1, "hot", pool));
        tiers.add(std::make_unique<MemoryTier>(2, "cold", pool));
    }
};

cppcoro::task<void> test_basic_write_then_read(Fixture& fx) {
    auto bytes = to_bytes("hello asyncmux");
    co_await fx.mux.write("/alpha", 0, std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/alpha", 0, bytes.size());
    assert_eq(to_string(out), "hello asyncmux", "basic write/read round-trip");
}

cppcoro::task<void> test_partial_read(Fixture& fx) {
    auto bytes = to_bytes("abcdefghijklmnop");
    co_await fx.mux.write("/beta", 0, std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/beta", 3, 5);
    assert_eq(to_string(out), "defgh", "partial read returns requested slice");
}

cppcoro::task<void> test_multi_block_round_trip(Fixture& fx) {
    std::string large(9000, 'x');
    for (int i = 0; i < 26; ++i) {
        large[300 + i] = static_cast<char>('A' + i);
        large[5000 + i] = static_cast<char>('a' + i);
    }

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/gamma", 0, std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/gamma", 0, bytes.size());
    assert_eq(to_string(out), large, "multi-block write/read round-trip");
}

cppcoro::task<void> test_cross_block_partial_read(Fixture& fx) {
    std::string large(5000, 'q');
    large[4094] = 'X';
    large[4095] = 'Y';
    large[4096] = 'Z';
    large[4097] = 'W';

    auto bytes = to_bytes(large);
    co_await fx.mux.write("/delta", 0, std::span<const Byte>(bytes.data(), bytes.size()));

    IoBuffer out = co_await fx.mux.read("/delta", 4094, 4);
    assert_eq(to_string(out), "XYZW", "read spanning block boundary");
}

cppcoro::task<void> test_promote_updates_metadata(Fixture& fx) {
    auto bytes = to_bytes("promote-me");
    co_await fx.mux.write("/epsilon", 0, std::span<const Byte>(bytes.data(), bytes.size()));

    BlockId block = fx.metadata.lookup("/epsilon", 0, bytes.size()).at(0).block_id;
    assert_true(fx.metadata.tier_of(block) == 1, "block starts on hot tier by default");

    co_await fx.mux.migrate(block, 1, 2);
    assert_true(fx.metadata.tier_of(block) == 2, "migrate updates metadata to destination tier");

    co_await fx.mux.promote(block, 1);
    assert_true(fx.metadata.tier_of(block) == 1, "promote moves block back to requested tier");

    IoBuffer out = co_await fx.mux.read("/epsilon", 0, bytes.size());
    assert_eq(to_string(out), "promote-me", "data survives migrate + promote");
}

cppcoro::task<void> test_missing_file_read_returns_empty(Fixture& fx) {
    IoBuffer out = co_await fx.mux.read("/does-not-exist", 0, 32);
    assert_true(out.data.size() == 32, "current scaffold zero-fills missing file reads to requested size");
    for (Byte b : out.data) {
        assert_true(b == Byte{0}, "missing file read should currently return zeros");
    }
}

cppcoro::task<void> test_overwrite_appends_new_metadata_in_current_scaffold(Fixture& fx) {
    auto first = to_bytes("AAAA");
    auto second = to_bytes("BBBB");

    co_await fx.mux.write("/zeta", 0, std::span<const Byte>(first.data(), first.size()));
    co_await fx.mux.write("/zeta", 0, std::span<const Byte>(second.data(), second.size()));

    auto locs = fx.metadata.lookup("/zeta", 0, 4);
    assert_true(locs.size() >= 2, "current scaffold records multiple versions on overlapping writes");
}

cppcoro::task<void> run_all_tests(Fixture& fx) {
    co_await test_basic_write_then_read(fx);
    co_await test_partial_read(fx);
    co_await test_multi_block_round_trip(fx);
    co_await test_cross_block_partial_read(fx);
    co_await test_promote_updates_metadata(fx);
    co_await test_missing_file_read_returns_empty(fx);
    co_await test_overwrite_appends_new_metadata_in_current_scaffold(fx);
}

} // namespace

int main() {
    try {
        Fixture fx;
        cppcoro::sync_wait(run_all_tests(fx));
        std::cout << kColorBoldGreen << "All AsyncMux scaffold tests passed." << kColorReset << "\n";
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
