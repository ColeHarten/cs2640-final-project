#ifndef ASYNCMUX_TIER_HH
#define ASYNCMUX_TIER_HH

#include "asyncmux.hh"

#include <cppcoro/task.hpp>

#include <memory>
#include <string>
#include <span>
#include <unordered_map>

namespace asyncmux {

class Tier {
public:
    virtual ~Tier() = default;

    virtual TierId id() const = 0;
    virtual std::string name() const = 0;

    virtual cppcoro::task<IoBuffer> read_at(const std::string& relative_path,
                                            uint64_t offset,
                                            uint64_t size) = 0;

    virtual cppcoro::task<void> write_at(const std::string& relative_path,
                                         uint64_t offset,
                                         asyncmux::span<const Byte> data) = 0;

    virtual cppcoro::task<void> remove_file(const std::string& relative_path) = 0;
};

class TierRegistry {
public:
    void add(std::unique_ptr<Tier> tier);
    Tier& get(TierId id);
    const Tier& get(TierId id) const;

private:
    std::unordered_map<TierId, std::unique_ptr<Tier>> tiers_;
};

} // namespace asyncmux

#endif