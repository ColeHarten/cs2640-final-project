#ifndef ASYNCMUX_PLACEMENT_POLICY_HH
#define ASYNCMUX_PLACEMENT_POLICY_HH

#include "asyncmux.hh"

namespace asyncmux {

class PlacementPolicy {
public:
    virtual ~PlacementPolicy() = default;
    virtual TierId choose_tier(const WriteBlock& block) = 0;
};

class ConstantPlacementPolicy final : public PlacementPolicy {
public:
    explicit ConstantPlacementPolicy(TierId default_tier);
    TierId choose_tier(const WriteBlock&) override;

private:
    TierId default_tier_;
};

class MutablePlacementPolicy final : public PlacementPolicy {
public:
    explicit MutablePlacementPolicy(TierId initial_tier) : tier_(initial_tier) {}

    void set(TierId tier) {
        tier_ = tier;
    }

    TierId choose_tier(const WriteBlock&) override {
        return tier_;
    }

private:
    TierId tier_;
};

} // namespace asyncmux

#endif