#ifndef ASYNC_MUX_HH
#define ASYNC_MUX_HH

#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../span.hh"

namespace amux {

using BlockId = std::uint64_t;
using TierId = std::uint32_t;

static constexpr std::size_t kBlockSize = 4096;

struct IoBuffer {
    std::vector<std::byte> data;

    IoBuffer();
    explicit IoBuffer(std::size_t n);
    explicit IoBuffer(std::vector<std::byte> bytes);

    std::size_t size() const;
    std::byte* bytes();
    const std::byte* bytes() const;
};

struct BlockLocation {
    BlockId block_id;
    TierId tier_id;
    std::uint64_t file_offset;
    std::uint64_t size;
};

struct WriteBlock {
    BlockId block_id;
    std::uint64_t file_offset;
    std::vector<std::byte> data;
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

class Tier {
public:
    virtual ~Tier() = default;

    virtual TierId id() const = 0;
    virtual std::string name() const = 0;

    virtual cppcoro::task<IoBuffer> read_at(const std::string& relative_path,
                                            std::uint64_t offset,
                                            std::uint64_t size) = 0;

    virtual cppcoro::task<void> write_at(const std::string& relative_path,
                                         std::uint64_t offset,
                                         span<const std::byte> data) = 0;

    virtual cppcoro::task<void> punch_hole(const std::string& relative_path,
                                           std::uint64_t offset,
                                           std::uint64_t size) = 0;

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

class FileSystemTier final : public Tier {
public:
    FileSystemTier(TierId id,
                   std::string name,
                   std::filesystem::path root_dir,
                   cppcoro::static_thread_pool& pool);

    TierId id() const override;
    std::string name() const override;

    cppcoro::task<IoBuffer> read_at(const std::string& relative_path,
                                    std::uint64_t offset,
                                    std::uint64_t size) override;

    cppcoro::task<void> write_at(const std::string& relative_path,
                                 std::uint64_t offset,
                                 span<const std::byte> data) override;

    cppcoro::task<void> punch_hole(const std::string& relative_path,
                                   std::uint64_t offset,
                                   std::uint64_t size) override;

    cppcoro::task<void> remove_file(const std::string& relative_path) override;

private:
    std::filesystem::path full_path(const std::string& relative_path) const;
    void ensure_parent_dirs(const std::filesystem::path& p) const;
    std::shared_ptr<std::shared_mutex> lock_for_path(const std::filesystem::path& p) const;

    TierId id_;
    std::string name_;
    std::filesystem::path root_dir_;
    cppcoro::static_thread_pool& pool_;
    mutable std::mutex lock_table_mu_;
    mutable std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> file_locks_;
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

    void relocate_block(BlockId block_id, TierId new_tier);

    LocatedBlock get_block(BlockId block_id) const;
    TierId tier_of(BlockId block_id) const;

    std::uint64_t version_of(const std::string& path) const;
    std::uint64_t bump_version(const std::string& path);

private:
    struct FileEntry {
        std::vector<BlockLocation> extents;
        std::uint64_t version = 0;
    };

    struct BlockIndexEntry {
        std::string path;
        std::uint64_t file_offset = 0;
        std::uint64_t size = 0;
    };

    void rebuild_block_index_locked(const std::string& path);

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, FileEntry> file_map_;
    std::unordered_map<BlockId, BlockIndexEntry> block_index_;
    BlockAllocator extent_allocator_;
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
    explicit MutablePlacementPolicy(TierId initial_tier)
        : tier_(initial_tier) {}

    void set(TierId tier) {
        tier_ = tier;
    }

    TierId choose_tier(const WriteBlock&) override {
        return tier_;
    }

private:
    TierId tier_;
};

class AsyncMux {
public:
    AsyncMux(TierRegistry& tiers,
             MetadataStore& metadata,
             PlacementPolicy& placement,
             cppcoro::static_thread_pool& pool,
             bool auto_migration_enabled = false,
             TierId hot_tier_id = 0);

    ~AsyncMux();

    cppcoro::task<IoBuffer> read(const std::string& path,
                                 std::uint64_t offset,
                                 std::uint64_t size);

    cppcoro::task<void> write(const std::string& path,
                              std::uint64_t offset,
                              span<const std::byte> data);

    cppcoro::task<void> migrate(BlockId block_id, TierId src_id, TierId dst_id);
    cppcoro::task<void> promote(BlockId block_id, TierId hot_tier);

private:
    struct PreparedWrite {
        WriteBlock block;
        TierId tier_id;
    };

    void enqueue_background_promotion(BlockId block_id);
    void background_worker_loop();

    std::vector<WriteBlock> split(std::uint64_t offset, span<const std::byte> data);

    IoBuffer assemble(const std::vector<BlockLocation>& locations,
                      std::vector<IoBuffer> pieces,
                      std::uint64_t read_offset,
                      std::uint64_t read_size);

    TierRegistry& tiers_;
    MetadataStore& metadata_;
    PlacementPolicy& placement_;
    cppcoro::static_thread_pool& pool_;
    BlockAllocator allocator_;
    bool auto_migration_enabled_ = false;
    TierId hot_tier_id_ = 0;

    std::thread bg_thread_;
    std::mutex bg_mu_;
    std::condition_variable bg_cv_;
    bool bg_stop_ = false;
    std::queue<BlockId> bg_queue_;
    std::unordered_set<BlockId> bg_queued_;
};

}  // namespace asyncmux

#endif