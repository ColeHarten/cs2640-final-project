#ifndef ASYNC_MUX_HH
#define ASYNC_MUX_HH

#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <span>
#include <string>
#include <vector>

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
                              std::span<const Byte> data);

    cppcoro::task<void> migrate(BlockId block_id, TierId src_id, TierId dst_id);

    cppcoro::task<void> promote(BlockId block_id, TierId hot_tier);

private:
    struct PreparedWrite {
        WriteBlock block;
        TierId tier_id;
    };

    std::vector<WriteBlock> split(uint64_t offset, std::span<const Byte> data);

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

class FuseFrontend {
public:
    explicit FuseFrontend(AsyncMux& mux);

    cppcoro::task<IoBuffer> on_read(const std::string& path,
                                    uint64_t offset,
                                    uint64_t size);

    cppcoro::task<void> on_write(const std::string& path,
                                 uint64_t offset,
                                 std::span<const Byte> data);

private:
    AsyncMux& mux_;
};

} // namespace asyncmux

#include "metadata_store.hh"
#include "placement_policy.hh"
#include "tier.hh"

#endif
