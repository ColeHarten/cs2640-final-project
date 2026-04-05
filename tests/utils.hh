#ifndef ASYNCMUX_TEST_UTILS_HH
#define ASYNCMUX_TEST_UTILS_HH

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

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

inline void assert_eq(const std::string& actual,
                      const std::string& expected,
                      const std::string& message) {
    if (actual != expected) {
        std::cerr << kColorRed << "FAIL:" << kColorReset << " " << message << "\n"
                  << "  expected: [" << expected << "]\n"
                  << "  actual:   [" << actual << "]\n";
        std::abort();
    }
}

inline void assert_true(bool cond, const std::string& message) {
    if (!cond) {
        std::cerr << kColorRed << "FAIL:" << kColorReset << " " << message << "\n";
        std::abort();
    }
}

#endif