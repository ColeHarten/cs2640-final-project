#include "amux/asyncmux.hh"

namespace asyncmux {

// MemoryTier::MemoryTier
//         Creates an in-memory tier bound to a scheduler pool.
MemoryTier::MemoryTier(TierId id, std::string name, cppcoro::static_thread_pool& pool)
    : id_(id), name_(std::move(name)), pool_(pool) {}

// MemoryTier::id
//         Returns this tier's unique identifier.
TierId MemoryTier::id() const {
    return id_;
}

// MemoryTier::name
//         Returns this tier's display name.
std::string MemoryTier::name() const {
    return name_;
}

// MemoryTier::read_block
//         Reads a block slice from in-memory storage.
cppcoro::task<IoBuffer> MemoryTier::read_block(BlockId block_id,
                                                uint64_t offset,
                                                uint64_t size) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    auto it = blocks_.find(block_id);
    if (it == blocks_.end()) {
        throw IoError("read_block: missing block");
    }
    if (offset > it->second.size()) {
        throw IoError("read_block: offset out of range");
    }

    const auto available = it->second.size() - static_cast<size_t>(offset);
    const auto n = std::min<size_t>(size, available);
    std::vector<Byte> out(n);
    std::copy_n(it->second.data() + offset, n, out.data());
    co_return IoBuffer{std::move(out)};
}

// MemoryTier::write_block
//         Writes bytes into a block at the requested offset.
cppcoro::task<void> MemoryTier::write_block(BlockId block_id,
                                             uint64_t offset,
                                             std::span<const Byte> data) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    auto& dst = blocks_[block_id];
    const size_t needed = static_cast<size_t>(offset) + data.size();
    if (dst.size() < needed) {
        dst.resize(needed);
    }
    std::copy(data.begin(), data.end(), dst.begin() + offset);
    co_return;
}

// MemoryTier::delete_block
//         Removes a block from in-memory storage.
cppcoro::task<void> MemoryTier::delete_block(BlockId block_id) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    blocks_.erase(block_id);
    co_return;
}

} // namespace asyncmux
