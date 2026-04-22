#ifndef ASYNC_MUX_HH
#define ASYNC_MUX_HH

#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <stdexcept>
#include <span>
#include <string>
#include <vector>

#include "span.hh"

namespace asyncmux {

using Byte = std::byte;
using size_t = std::size_t;
using uint64_t = std::uint64_t;
using BlockId = uint64_t;
using TierId = std::uint32_t;

static constexpr size_t kBlockSize = 4096;

struct IoBuffer {
    std::vector<Byte> data;

    IoBuffer();
    explicit IoBuffer(size_t n);
    explicit IoBuffer(std::vector<Byte> bytes);

    size_t size() const;
    Byte* bytes();
    const Byte* bytes() const;
};

struct BlockLocation {
    BlockId block_id;
    TierId tier_id;
    uint64_t file_offset;
    uint64_t size;
};

struct WriteBlock {
    BlockId block_id;
    uint64_t file_offset;
    std::vector<Byte> data;
};

class IoError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct LocatedBlock {
    std::string path;
    BlockLocation location;
};

class BlockAllocator {
public:
    BlockId next() {
        return next_id_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    std::atomic<BlockId> next_id_{1};
};

class MetadataStore;
class PlacementPolicy;
class TierRegistry;

} // namespace asyncmux

namespace asyncmux {

class AsyncMux {
public:
    AsyncMux(TierRegistry& tiers,
             MetadataStore& metadata,
             PlacementPolicy& placement,
             cppcoro::static_thread_pool& pool,
             bool auto_migration_enabled = false);

    cppcoro::task<IoBuffer> read(const std::string& path,
                                 uint64_t offset,
                                 uint64_t size);

    cppcoro::task<void> write(const std::string& path,
                              uint64_t offset,
                              asyncmux::span<const Byte> data);

    cppcoro::task<void> migrate(BlockId block_id, TierId src_id, TierId dst_id);

    cppcoro::task<void> promote(BlockId block_id, TierId hot_tier);

private:
    struct PreparedWrite {
        WriteBlock block;
        TierId tier_id;
    };

    std::vector<WriteBlock> split(uint64_t offset, asyncmux::span<const Byte> data);

    IoBuffer assemble(const std::vector<BlockLocation>& locations,
                      std::vector<IoBuffer> pieces,
                      uint64_t read_offset,
                      uint64_t read_size);

    TierRegistry& tiers_;
    MetadataStore& metadata_;
    PlacementPolicy& placement_;
    cppcoro::static_thread_pool& pool_;
    BlockAllocator allocator_;
    bool auto_migration_enabled_ = false;
};

class MetadataStore {
public:
    std::vector<BlockLocation> lookup(const std::string& path,
                                      uint64_t offset,
                                      uint64_t size) const;

    void update(const std::string& path,
                BlockId block_id,
                TierId tier_id,
                uint64_t file_offset,
                uint64_t size);

    void update_block(BlockId block_id, TierId new_tier);

    void relocate_block(BlockId block_id, TierId new_tier);

    LocatedBlock get_block(BlockId block_id) const;

    TierId tier_of(BlockId block_id) const;

    uint64_t version_of(const std::string& path) const;
    uint64_t bump_version(const std::string& path);

private:
    struct FileEntry {
        std::vector<BlockLocation> extents;
        uint64_t version = 0;
    };

    struct BlockIndexEntry {
        std::string path;
        uint64_t file_offset = 0;
        uint64_t size = 0;
    };

    void rebuild_block_index_locked(const std::string& path);

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, FileEntry> file_map_;
    std::unordered_map<BlockId, BlockIndexEntry> block_index_;
};

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

class FuseFrontend {
public:
    explicit FuseFrontend(AsyncMux& mux);

    cppcoro::task<IoBuffer> on_read(const std::string& path,
                                    uint64_t offset,
                                    uint64_t size);

    cppcoro::task<void> on_write(const std::string& path,
                                 uint64_t offset,
                                 asyncmux::span<const Byte> data);

private:
    AsyncMux& mux_;
};

} // namespace asyncmux
#include "tier.hh"

#endif
