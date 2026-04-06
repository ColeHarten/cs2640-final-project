#include "placement_policy.hh"

namespace asyncmux {

ConstantPlacementPolicy::ConstantPlacementPolicy(TierId default_tier)
    : default_tier_(default_tier) {}

TierId ConstantPlacementPolicy::choose_tier(const WriteBlock&) {
    return default_tier_;
}

} // namespace asyncmux