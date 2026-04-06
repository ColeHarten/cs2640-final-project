#include "tier.hh"

namespace asyncmux {

void TierRegistry::add(std::unique_ptr<Tier> tier) {
    auto [_, inserted] = tiers_.emplace(tier->id(), std::move(tier));
    if (!inserted) {
        throw IoError("duplicate tier id");
    }
}

Tier& TierRegistry::get(TierId id) {
    auto it = tiers_.find(id);
    if (it == tiers_.end()) {
        throw IoError("unknown tier");
    }
    return *it->second;
}

const Tier& TierRegistry::get(TierId id) const {
    auto it = tiers_.find(id);
    if (it == tiers_.end()) {
        throw IoError("unknown tier");
    }
    return *it->second;
}

} // namespace asyncmux