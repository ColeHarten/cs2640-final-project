#ifndef ASYNCMUX_TEST_UTILS_HH
#define ASYNCMUX_TEST_UTILS_HH

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#ifdef __linux__
#include <execinfo.h>
#endif

#include "amux/asyncmux.hh"

using asyncmux::Byte;
using asyncmux::IoBuffer;

inline constexpr const char* kColorRed = "\x1b[31m";
inline constexpr const char* kColorBoldGreen = "\x1b[1;32m";
inline constexpr const char* kColorGreen = "\x1b[32m";
inline constexpr const char* kColorReset = "\x1b[0m";

inline std::vector<Byte> to_bytes(const std::string& s) {
    std::vector<Byte> out;
    out.reserve(s.size());
    for (unsigned char ch : s) {
        out.push_back(static_cast<Byte>(ch));
    }
    return out;
}

inline std::string to_string(const IoBuffer& buf) {
    std::string out;
    out.reserve(buf.data.size());
    for (Byte b : buf.data) {
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