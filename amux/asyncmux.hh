#ifndef ASYNC_MUX_HH
#define ASYNC_MUX_HH

#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/static_thread_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
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
using BlockId = std::uint64_t;
using TierId = std::uint32_t;

static constexpr std::size_t kBlockSize = 4096;

struct IoBuffer {
    std::vector<Byte> data;

    IoBuffer();
    explicit IoBuffer(std::size_t n);
    explicit IoBuffer(std::vector<Byte> bytes);

    std::size_t size() const;
    Byte* bytes();
    const Byte* bytes() const;
};

struct BlockLocation {
    BlockId block_id;
    TierId tier_id;
    std::uint64_t file_offset;
    std::uint64_t block_offset;
    std::uint64_t size;
};

struct WriteBlock {
    BlockId block_id;
    std::uint64_t file_offset;
    std::vector<Byte> data;
};

class IoError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class MetadataStore {
public:
    std::vector<BlockLocation> lookup(const std::string& path,
                                      std::uint64_t offset,
                                      std::uint64_t size) const;

    void update(const std::string& path,
                BlockId block_id,
                TierId tier_id,
                std::uint64_t file_offset,
                std::uint64_t size);

    void update_block(BlockId block_id, TierId new_tier);

    TierId tier_of(BlockId block_id) const;

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::vector<BlockLocation>> file_map_;
};

class Tier {
public:
    virtual ~Tier() = default;

    virtual TierId id() const = 0;
    virtual std::string name() const = 0;

    virtual cppcoro::task<IoBuffer> read_block(BlockId block_id,
                                               std::uint64_t offset,
                                               std::uint64_t size) = 0;

    virtual cppcoro::task<void> write_block(BlockId block_id,
                                            std::uint64_t offset,
                                            std::span<const Byte> data) = 0;

    virtual cppcoro::task<void> delete_block(BlockId block_id) = 0;
};

class MemoryTier final : public Tier {
public:
    MemoryTier(TierId id, std::string name, cppcoro::static_thread_pool& pool);

    TierId id() const override;
    std::string name() const override;

    cppcoro::task<IoBuffer> read_block(BlockId block_id,
                                       std::uint64_t offset,
                                       std::uint64_t size) override;

    cppcoro::task<void> write_block(BlockId block_id,
                                    std::uint64_t offset,
                                    std::span<const Byte> data) override;

    cppcoro::task<void> delete_block(BlockId block_id) override;

private:
    TierId id_;
    std::string name_;
    cppcoro::static_thread_pool& pool_;
    std::mutex mu_;
    std::unordered_map<BlockId, std::vector<Byte>> blocks_;
};

class TierRegistry {
public:
    void add(std::unique_ptr<Tier> tier);

    Tier& get(TierId id);

    const Tier& get(TierId id) const;

private:
    std::unordered_map<TierId, std::unique_ptr<Tier>> tiers_;
};

class PlacementPolicy {
public:
    virtual ~PlacementPolicy() = default;
    virtual TierId choose_tier(const WriteBlock& block) = 0;
};

class SimplePlacementPolicy final : public PlacementPolicy {
public:
    explicit SimplePlacementPolicy(TierId default_tier);

    TierId choose_tier(const WriteBlock&) override;

private:
    TierId default_tier_;
};

class BlockAllocator {
public:
    BlockId next();

private:
    BlockId next_id_ = 1;
};

class AsyncMux {
public:
    AsyncMux(TierRegistry& tiers,
             MetadataStore& metadata,
             PlacementPolicy& placement,
             cppcoro::static_thread_pool& pool);

    cppcoro::task<IoBuffer> read(const std::string& path,
                                 std::uint64_t offset,
                                 std::uint64_t size);

    cppcoro::task<void> write(const std::string& path,
                              std::uint64_t offset,
                              std::span<const Byte> data);

    cppcoro::task<void> migrate(BlockId block_id, TierId src_id, TierId dst_id);

    cppcoro::task<void> promote(BlockId block_id, TierId hot_tier);

private:
    std::vector<WriteBlock> split(std::uint64_t offset, std::span<const Byte> data);

    IoBuffer assemble(const std::vector<BlockLocation>& locations,
                      std::vector<IoBuffer> blocks,
                      std::uint64_t read_offset,
                      std::uint64_t read_size);

    TierRegistry& tiers_;
    MetadataStore& metadata_;
    PlacementPolicy& placement_;
    cppcoro::static_thread_pool& pool_;
    BlockAllocator allocator_;
};

class FuseFrontend {
public:
    explicit FuseFrontend(AsyncMux& mux);

    cppcoro::task<IoBuffer> on_read(const std::string& path,
                                    std::uint64_t offset,
                                    std::uint64_t size);

    cppcoro::task<void> on_write(const std::string& path,
                                 std::uint64_t offset,
                                 std::span<const Byte> data);

private:
    AsyncMux& mux_;
};

} // namespace asyncmux

#endif