#ifndef ASYNC_MUX_HH
#define ASYNC_MUX_HH

#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/static_thread_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace asyncmux {

using Byte = std::byte;
using size_t = std::size_t;
using uint64_t = std::uint64_t;
using BlockId = uint64_t;
using TierId = std::uint32_t;


static constexpr size_t kBlockSize = 4096;

// Value type that owns a contiguous byte buffer for read and write results.
struct IoBuffer {
    std::vector<Byte> data;

    IoBuffer();
    explicit IoBuffer(size_t n);
    explicit IoBuffer(std::vector<Byte> bytes);

    size_t size() const;
    Byte* bytes();
    const Byte* bytes() const;
};

// Describes where a file range is stored within a tier and block.
struct BlockLocation {
    BlockId block_id;
    TierId tier_id;
    uint64_t file_offset;
    uint64_t block_offset;
    uint64_t size;
};

// Write request payload describing a block that should be persisted.
struct WriteBlock {
    BlockId block_id;
    uint64_t file_offset;
    std::vector<Byte> data;
};

// Exception type used for I/O and storage-related failures.
class IoError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Thread-safe metadata store that tracks which blocks belong to which file path.
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

    TierId tier_of(BlockId block_id) const;

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::vector<BlockLocation>> file_map_;
};

// Storage-tier interface implemented by concrete backends.
class Tier {
public:
    virtual ~Tier() = default;

    virtual TierId id() const = 0;
    virtual std::string name() const = 0;

    virtual cppcoro::task<IoBuffer> read_block(BlockId block_id,
                                                              uint64_t offset,
                                                              uint64_t size) = 0;

    virtual cppcoro::task<void> write_block(BlockId block_id,
                                                          uint64_t offset,
                                            std::span<const Byte> data) = 0;

    virtual cppcoro::task<void> delete_block(BlockId block_id) = 0;
};

// In-memory storage-tier implementation derived from Tier.
class MemoryTier final : public Tier {
public:
    MemoryTier(TierId id, std::string name, cppcoro::static_thread_pool& pool);

    TierId id() const override;
    std::string name() const override;

    cppcoro::task<IoBuffer> read_block(BlockId block_id,
                                                    uint64_t offset,
                                                    uint64_t size) override;

    cppcoro::task<void> write_block(BlockId block_id,
                                                uint64_t offset,
                                    std::span<const Byte> data) override;

    cppcoro::task<void> delete_block(BlockId block_id) override;

private:
    TierId id_;
    std::string name_;
    cppcoro::static_thread_pool& pool_;
    std::mutex mu_;
    std::unordered_map<BlockId, std::vector<Byte>> blocks_;
};

// Disk-backed storage-tier implementation derived from Tier.
class DiskTier final : public Tier {
public:
    DiskTier(TierId id,
             std::string name,
             std::filesystem::path root_dir,
             cppcoro::static_thread_pool& pool);

    TierId id() const override;
    std::string name() const override;

    cppcoro::task<IoBuffer> read_block(BlockId block_id,
                                       uint64_t offset,
                                       uint64_t size) override;

    cppcoro::task<void> write_block(BlockId block_id,
                                    uint64_t offset,
                                    std::span<const Byte> data) override;

    cppcoro::task<void> delete_block(BlockId block_id) override;

private:
    std::filesystem::path block_path(BlockId block_id) const;

    TierId id_;
    std::string name_;
    std::filesystem::path root_dir_;
    cppcoro::static_thread_pool& pool_;
    std::mutex mu_;
};

// Owns and resolves Tier instances by tier id.
class TierRegistry {
public:
    void add(std::unique_ptr<Tier> tier);

    Tier& get(TierId id);

    const Tier& get(TierId id) const;

private:
    std::unordered_map<TierId, std::unique_ptr<Tier>> tiers_;
};

// Placement-policy interface used by AsyncMux to choose a destination tier.
class PlacementPolicy {
public:
    virtual ~PlacementPolicy() = default;
    virtual TierId choose_tier(const WriteBlock& block) = 0;
};

// SimplePlacementPolicy is a concrete PlacementPolicy that always chooses one tier.
class SimplePlacementPolicy final : public PlacementPolicy {
public:
    explicit SimplePlacementPolicy(TierId default_tier);

    TierId choose_tier(const WriteBlock&) override;

private:
    TierId default_tier_;
};

// Monotonic block-id allocator used by AsyncMux for new blocks.
class BlockAllocator {
public:
    BlockId next();

private:
    BlockId next_id_ = 1;
};

// Core async coordinator that routes reads, writes, migration, and promotion across tiers.
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
    std::vector<WriteBlock> split(uint64_t offset, std::span<const Byte> data);

    IoBuffer assemble(const std::vector<BlockLocation>& locations,
                      std::vector<IoBuffer> blocks,
                      uint64_t read_offset,
                      uint64_t read_size);

    TierRegistry& tiers_;
    MetadataStore& metadata_;
    PlacementPolicy& placement_;
    cppcoro::static_thread_pool& pool_;
    BlockAllocator allocator_;
    bool auto_migration_enabled_ = false;
};

// Thin coroutine-friendly frontend wrapper around AsyncMux.
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

#endif