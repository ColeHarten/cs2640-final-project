#ifndef ASYNCMUX_TEST_UTILS_HH
#define ASYNCMUX_TEST_UTILS_HH

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>

#include "../span.hh"

#ifdef __linux__
#include <execinfo.h>
#endif

namespace fs = std::filesystem;

inline const fs::path kHotRoot = "/tier0/tmpfs";
inline const fs::path kWarmRoot = "/tier1/data";
inline const fs::path kColdRoot = "/tier2/data";

inline constexpr long kTmpfsMagic = 0x01021994;
inline constexpr long kExtMagic = 0xEF53;
inline constexpr long kXfsMagic = 0x58465342;

inline constexpr const char* kColorRed = "\x1b[31m";
inline constexpr const char* kColorBoldGreen = "\x1b[1;32m";
inline constexpr const char* kColorGreen = "\x1b[32m";
inline constexpr const char* kColorReset = "\x1b[0m";

inline std::vector<std::byte> to_bytes(const std::string& s) {
    std::vector<std::byte> out;
    out.reserve(s.size());
    for (unsigned char ch : s) {
        out.push_back(static_cast<std::byte>(ch));
    }
    return out;
}

inline long fs_magic_for(const fs::path& root) {
    struct statfs stat_info {};
    if (statfs(root.c_str(), &stat_info) != 0) {
        throw std::runtime_error("statfs failed for " + root.string());
    }
    return static_cast<long>(stat_info.f_type);
}

inline bool is_mountpoint(const fs::path& root) {
    std::error_code ec;
    if (!fs::exists(root, ec) || ec) {
        return false;
    }

    struct stat root_stat {};
    struct stat parent_stat {};

    if (::stat(root.c_str(), &root_stat) != 0) {
        return false;
    }

    const fs::path parent = root.parent_path();
    if (parent.empty()) {
        return true;
    }

    if (::stat(parent.c_str(), &parent_stat) != 0) {
        return false;
    }

    return (root_stat.st_dev != parent_stat.st_dev) ||
           (root_stat.st_ino == parent_stat.st_ino);
}

inline void ensure_dir_exists(const fs::path& root) {
    std::error_code ec;
    if (!fs::exists(root, ec) || ec) {
        throw std::runtime_error("expected tier path does not exist: " + root.string());
    }
    if (!fs::is_directory(root, ec) || ec) {
        throw std::runtime_error("expected tier path is not a directory: " + root.string());
    }
}

inline void wait_for_mount(const fs::path& root, const std::string& name) {
    for (int i = 0; i < 30; ++i) {
        std::error_code ec;
        if (fs::exists(root, ec) && !ec && is_mountpoint(root)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    throw std::runtime_error("timed out waiting for " + name + " tier mount: " + root.string());
}

inline std::string normalize_test_path(const std::string& path) {
    fs::path p(path);
    p = p.lexically_normal();
    if (p.is_absolute()) {
        p = p.lexically_relative("/");
    }
    return p.string();
}

template <typename MetadataStoreT>
inline auto sorted_locs(MetadataStoreT& metadata,
                        const std::string& path,
                        std::uint64_t offset,
                        std::uint64_t size) {
    auto locs = metadata.lookup(normalize_test_path(path), offset, size);
    std::sort(locs.begin(), locs.end(),
              [](const auto& a, const auto& b) {
                  return a.file_offset < b.file_offset;
              });
    return locs;
}

template <typename IoBufferT>
inline std::string to_string(const IoBufferT& buf) {
    std::string out;
    out.reserve(buf.data.size());
    for (auto b : buf.data) {
        out.push_back(static_cast<char>(b));
    }
    return out;
}

inline void print_backtrace(std::ostream& os, std::size_t skip_frames = 0) {
#ifdef __linux__
    void* frames[64] = {};
    const int frame_count = ::backtrace(frames, 64);
    char** symbols = ::backtrace_symbols(frames, frame_count);

    os << "  backtrace:\n";
    if (symbols == nullptr) {
        os << "    <unavailable: backtrace_symbols returned null>\n";
        return;
    }

    for (int i = static_cast<int>(skip_frames); i < frame_count; ++i) {
        os << "    [" << (i - static_cast<int>(skip_frames)) << "] " << symbols[i] << "\n";
    }
    std::free(symbols);
#else
    (void)skip_frames;
    os << "  backtrace: <unsupported on this platform>\n";
#endif
}

[[noreturn]] inline void fail_assert(const std::string& message,
                                     const char* file,
                                     int line,
                                     const char* function,
                                     const std::string& details = std::string()) {
    std::cerr << kColorRed << "ERROR" << kColorReset << " [assertion_failure]\n"
              << "  message: " << message << "\n"
              << "  location: " << file << ":" << line << "\n"
              << "  function: " << function << "\n";
    if (!details.empty()) {
        std::cerr << "  details:\n" << details;
    }
    print_backtrace(std::cerr, 2);
    std::abort();
}

inline void log_uncaught_exception(const std::exception& ex,
                                   const char* file,
                                   int line,
                                   const char* function) {
    std::cerr << kColorRed << "ERROR" << kColorReset << " [uncaught_exception]\n"
              << "  message: " << ex.what() << "\n"
              << "  location: " << file << ":" << line << "\n"
              << "  function: " << function << "\n";
    print_backtrace(std::cerr, 2);
}

inline void log_uncaught_nonstd(const char* file,
                                int line,
                                const char* function) {
    std::cerr << kColorRed << "ERROR" << kColorReset << " [uncaught_nonstd_exception]\n"
              << "  message: uncaught non-standard exception\n"
              << "  location: " << file << ":" << line << "\n"
              << "  function: " << function << "\n";
    print_backtrace(std::cerr, 2);
}

inline void assert_eq_impl(const std::string& actual,
                           const std::string& expected,
                           const std::string& message,
                           const char* file,
                           int line,
                           const char* function) {
    if (actual != expected) {
        const std::string details = "    expected: [" + expected + "]\n" +
                                    "    actual:   [" + actual + "]\n";
        fail_assert(message, file, line, function, details);
    }
}

inline void assert_true_impl(bool cond,
                             const std::string& message,
                             const char* file,
                             int line,
                             const char* function) {
    if (!cond) {
        fail_assert(message, file, line, function);
    }
}

#define assert_eq(actual, expected, message) \
    assert_eq_impl((actual), (expected), (message), __FILE__, __LINE__, __func__)

#define assert_true(cond, message) \
    assert_true_impl((cond), (message), __FILE__, __LINE__, __func__)

#endif