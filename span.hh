#ifndef SPAN_HH
#define SPAN_HH

#include <cstddef>
#include <type_traits>
#include <vector>

template <typename T>
class span {
public:
    using element_type = T;
    using value_type = std::remove_cv_t<T>;
    using size_type = std::size_t;
    using pointer = T*;
    using reference = T&;
    using iterator = pointer;

    span() noexcept : ptr_(nullptr), len_(0) {}
    span(pointer ptr, size_type len) noexcept : ptr_(ptr), len_(len) {}

    template <typename U,
              typename = std::enable_if_t<std::is_convertible_v<U(*)[], T(*)[]>>>
    span(const std::vector<U>& v) noexcept
        : ptr_(v.data()), len_(v.size()) {}

    pointer data() const noexcept { return ptr_; }
    size_type size() const noexcept { return len_; }
    bool empty() const noexcept { return len_ == 0; }

    reference operator[](size_type i) const noexcept { return ptr_[i]; }

    iterator begin() const noexcept { return ptr_; }
    iterator end() const noexcept { return ptr_ + len_; }

    span<T> subspan(size_type offset) const noexcept {
        return span<T>(ptr_ + offset, len_ - offset);
    }

    span<T> subspan(size_type offset, size_type count) const noexcept {
        return span<T>(ptr_ + offset, count);
    }

private:
    pointer ptr_;
    size_type len_;
};

#endif