#ifndef BLOCKING_MUX_HH
#define BLOCKING_MUX_HH

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/falloc.h>
#endif

#include "../span.hh"

namespace bmux {

using BlockId = std::uint64_t;
using TierId = std::uint32_t;
static constexpr std::size_t kBlockSize = 4096;

struct IoBuffer {
    std::vector<std::byte> data;
    IoBuffer() = default;
    explicit IoBuffer(std::size_t n);
    explicit IoBuffer(std::vector<std::byte> d);
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

struct LocatedBlock {
    std::string path;
    BlockLocation location;
};

class IoError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class BlockAllocator {
public:
    BlockId next();

private:
    std::atomic<BlockId> next_id_{1};
};

class ThreadPool {
public:
    explicit ThreadPool(std::size_t nthreads);
    ~ThreadPool();
    void submit(std::function<void()> job);

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> jobs_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;
};

class Tier {
public:
    virtual ~Tier() = default;
    virtual TierId id() const = 0;
    virtual std::string name() const = 0;
    virtual IoBuffer read_at(const std::string& relative_path,
                             std::uint64_t offset,
                             std::uint64_t size) = 0;
    virtual void write_at(const std::string& relative_path,
                          std::uint64_t offset,
                          span<const std::byte> data) = 0;
    virtual void punch_hole(const std::string& relative_path,
                            std::uint64_t offset,
                            std::uint64_t size) = 0;
    virtual void remove_file(const std::string& relative_path) = 0;
};

class TierRegistry {
public:
    void add(std::unique_ptr<Tier> tier);
    Tier& get(TierId id);

private:
    std::unordered_map<TierId, std::unique_ptr<Tier>> tiers_;
};

class FileSystemTier final : public Tier {
public:
    FileSystemTier(TierId id,
                   std::string name,
                   std::filesystem::path root_dir);

    TierId id() const override;
    std::string name() const override;

    IoBuffer read_at(const std::string& relative_path,
                     std::uint64_t offset,
                     std::uint64_t size) override;

    void write_at(const std::string& relative_path,
                  std::uint64_t offset,
                  span<const std::byte> data) override;

    void punch_hole(const std::string& relative_path,
                    std::uint64_t offset,
                    std::uint64_t size) override;

    void remove_file(const std::string& relative_path) override;

private:
    std::filesystem::path full_path(const std::string& relative_path) const;
    void ensure_parent_dirs(const std::filesystem::path& p) const;
    std::shared_ptr<std::shared_mutex> lock_for_path(const std::filesystem::path& p) const;

    TierId id_;
    std::string name_;
    std::filesystem::path root_dir_;
    mutable std::mutex lock_mu_;
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

    static std::uint64_t checked_end(std::uint64_t off, std::uint64_t sz);

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, FileEntry> file_map_;
    std::unordered_map<BlockId, BlockIndexEntry> block_index_;
    BlockAllocator extent_alloc_;
};

class PlacementPolicy {
public:
    virtual ~PlacementPolicy() = default;
    virtual TierId choose_tier(const WriteBlock& block) = 0;
};

class MutablePlacementPolicy final : public PlacementPolicy {
public:
    explicit MutablePlacementPolicy(TierId initial);

    void set(TierId tier);

    TierId choose_tier(const WriteBlock&) override;

private:
    std::atomic<TierId> tier_;
};

class BlockingMux {
public:
    BlockingMux(TierRegistry& tiers,
                MetadataStore& metadata,
                PlacementPolicy& placement,
                std::size_t worker_threads,
                bool auto_migration_enabled = false,
                TierId hot_tier_id = 0);

    ~BlockingMux();

    IoBuffer read(const std::string& raw_path,
                  std::uint64_t offset,
                  std::uint64_t size);

    void write(const std::string& raw_path,
               std::uint64_t offset,
               span<const std::byte> data);

    void migrate(BlockId block_id, TierId src_id, TierId dst_id);
    void promote(BlockId block_id, TierId hot_tier);
    void wait_for_background_idle();

private:
    static std::uint64_t checked_end(std::uint64_t off, std::uint64_t sz);
    static std::string normalize_path(const std::string& path);
    void enqueue_promotion(BlockId block_id);
    void background_loop();
    std::vector<WriteBlock> split(std::uint64_t offset, span<const std::byte> data);
    IoBuffer assemble(const std::vector<BlockLocation>& locations,
                      const std::vector<IoBuffer>& pieces,
                      std::uint64_t read_offset,
                      std::uint64_t read_size);

    TierRegistry& tiers_;
    MetadataStore& metadata_;
    PlacementPolicy& placement_;
    ThreadPool pool_;
    BlockAllocator allocator_;
    bool auto_migration_enabled_ = false;
    TierId hot_tier_id_ = 0;

    std::thread bg_thread_;
    std::mutex bg_mu_;
    std::condition_variable bg_cv_;
    std::condition_variable bg_idle_cv_;
    bool bg_stop_ = false;
    std::size_t bg_active_jobs_ = 0;
    std::queue<BlockId> bg_queue_;
    std::unordered_set<BlockId> bg_queued_;
};

} // namespace bmux

#endif