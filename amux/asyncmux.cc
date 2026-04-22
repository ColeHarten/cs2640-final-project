// asyncmux.cc
#include "asyncmux.hh"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <sstream>
#include <system_error>
#include <unistd.h>

#ifdef __linux__
#include <linux/falloc.h>
#endif

#include <cppcoro/sync_wait.hpp>

namespace asyncmux {

namespace {

uint64_t checked_end(uint64_t offset, uint64_t size) {
    return (size > UINT64_MAX - offset) ? UINT64_MAX : (offset + size);
}

std::string io_error_with_context(const char* what,
                                  const std::filesystem::path& path,
                                  int saved_errno,
                                  const char* file,
                                  int line,
                                  const char* function) {
    std::ostringstream os;
    os << what
       << " | path='" << path.string() << "'"
       << " | errno=" << saved_errno
       << " (" << std::strerror(saved_errno) << ")"
       << " | at " << file << ":" << line
       << " in " << function;
    return os.str();
}

std::string fs_error_with_context(const char* what,
                                  const std::filesystem::path& path,
                                  const std::error_code& ec,
                                  const char* file,
                                  int line,
                                  const char* function) {
    std::ostringstream os;
    os << what
       << " | path='" << path.string() << "'"
       << " | error=" << ec.value()
       << " (" << ec.message() << ")"
       << " | at " << file << ":" << line
       << " in " << function;
    return os.str();
}

} // namespace

IoBuffer::IoBuffer() = default;
IoBuffer::IoBuffer(size_t n) : data(n) {}
IoBuffer::IoBuffer(std::vector<Byte> bytes) : data(std::move(bytes)) {}

size_t IoBuffer::size() const { return data.size(); }
Byte* IoBuffer::bytes() { return data.data(); }
const Byte* IoBuffer::bytes() const { return data.data(); }

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

FileSystemTier::FileSystemTier(TierId id,
                               std::string name,
                               std::filesystem::path root_dir,
                               cppcoro::static_thread_pool& pool)
    : id_(id),
      name_(std::move(name)),
      root_dir_(std::move(root_dir)),
      pool_(pool) {
    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        throw IoError(fs_error_with_context("filesystem tier root setup failed",
                                            root_dir_,
                                            ec,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }
}

TierId FileSystemTier::id() const { return id_; }
std::string FileSystemTier::name() const { return name_; }

std::filesystem::path FileSystemTier::full_path(const std::string& relative_path) const {
    std::filesystem::path rel(relative_path);
    if (rel.is_absolute()) {
        rel = rel.lexically_relative("/");
    }
    return root_dir_ / rel;
}

void FileSystemTier::ensure_parent_dirs(const std::filesystem::path& p) const {
    const auto parent = p.parent_path();
    if (!parent.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            throw IoError(fs_error_with_context("write_at: failed to create parent directories",
                                                parent,
                                                ec,
                                                __FILE__,
                                                __LINE__,
                                                __func__));
        }
    }
}

std::shared_ptr<std::shared_mutex> FileSystemTier::lock_for_path(const std::filesystem::path& p) const {
    const std::string key = p.lexically_normal().string();
    std::lock_guard<std::mutex> guard(lock_table_mu_);
    auto& entry = file_locks_[key];
    if (!entry) {
        entry = std::make_shared<std::shared_mutex>();
    }
    return entry;
}

cppcoro::task<IoBuffer> FileSystemTier::read_at(const std::string& relative_path,
                                                uint64_t offset,
                                                uint64_t size) {
    co_await pool_.schedule();

    const auto path = full_path(relative_path);
    const auto file_lock = lock_for_path(path);
    std::shared_lock<std::shared_mutex> lock(*file_lock);

    std::vector<Byte> out(static_cast<size_t>(size), Byte{0});

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (!std::filesystem::exists(path)) {
            co_return IoBuffer{std::move(out)};
        }
        throw IoError(io_error_with_context("read_at: failed to open file",
                                            path,
                                            errno ? errno : EIO,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    in.seekg(0, std::ios::end);
    const auto end_pos = in.tellg();
    if (end_pos < 0) {
        throw IoError(io_error_with_context("read_at: failed to determine file size",
                                            path,
                                            errno ? errno : EIO,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }
    const auto file_size = static_cast<uint64_t>(end_pos);

    if (offset >= file_size) {
        co_return IoBuffer{std::move(out)};
    }

    const uint64_t readable = std::min<uint64_t>(size, file_size - offset);
    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(readable));

    if (in.bad()) {
        throw IoError(io_error_with_context("read_at: failed during read",
                                            path,
                                            errno ? errno : EIO,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    co_return IoBuffer{std::move(out)};
}

cppcoro::task<void> FileSystemTier::write_at(const std::string& relative_path,
                                             uint64_t offset,
                                             asyncmux::span<const Byte> data) {
    co_await pool_.schedule();

    const auto path = full_path(relative_path);
    ensure_parent_dirs(path);

    const auto file_lock = lock_for_path(path);
    std::unique_lock<std::shared_mutex> lock(*file_lock);

    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        throw IoError(io_error_with_context("write_at: failed to open file",
                                            path,
                                            errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    const auto* ptr = reinterpret_cast<const unsigned char*>(data.data());
    size_t remaining = data.size();
    off_t pos = static_cast<off_t>(offset);

    while (remaining > 0) {
        const ssize_t written = ::pwrite(fd, ptr, remaining, pos);
        if (written < 0) {
            const int saved_errno = errno;
            ::close(fd);
            throw IoError(io_error_with_context("write_at: failed during write",
                                                path,
                                                saved_errno,
                                                __FILE__,
                                                __LINE__,
                                                __func__));
        }
        ptr += static_cast<size_t>(written);
        remaining -= static_cast<size_t>(written);
        pos += written;
    }

    if (::close(fd) != 0) {
        throw IoError(io_error_with_context("write_at: close failed",
                                            path,
                                            errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    co_return;
}

cppcoro::task<void> FileSystemTier::punch_hole(const std::string& relative_path,
                                               uint64_t offset,
                                               uint64_t size) {
    co_await pool_.schedule();

    if (size == 0) {
        co_return;
    }

    const auto path = full_path(relative_path);
    const auto file_lock = lock_for_path(path);
    std::unique_lock<std::shared_mutex> lock(*file_lock);

    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        if (errno == ENOENT) {
            co_return;
        }
        throw IoError(io_error_with_context("punch_hole: failed to open file",
                                            path,
                                            errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

#ifdef __linux__
    if (::fallocate(fd,
                    FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                    static_cast<off_t>(offset),
                    static_cast<off_t>(size)) != 0) {
        if (errno != EOPNOTSUPP && errno != ENOSYS) {
            const int saved_errno = errno;
            ::close(fd);
            throw IoError(io_error_with_context("punch_hole: fallocate failed",
                                                path,
                                                saved_errno,
                                                __FILE__,
                                                __LINE__,
                                                __func__));
        }
    }
#endif

    if (::close(fd) != 0) {
        throw IoError(io_error_with_context("punch_hole: close failed",
                                            path,
                                            errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    co_return;
}

cppcoro::task<void> FileSystemTier::remove_file(const std::string& relative_path) {
    co_await pool_.schedule();

    const auto path = full_path(relative_path);
    const auto file_lock = lock_for_path(path);
    std::unique_lock<std::shared_mutex> lock(*file_lock);

    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        throw IoError(fs_error_with_context("remove_file: failed to remove file",
                                            path,
                                            ec,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    co_return;
}

std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
                                                 uint64_t offset,
                                                 uint64_t size) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    if (it == file_map_.end() || size == 0) {
        return {};
    }

    std::vector<BlockLocation> out;
    const uint64_t end = checked_end(offset, size);
    for (const auto& loc : it->second.extents) {
        const uint64_t loc_begin = loc.file_offset;
        const uint64_t loc_end = checked_end(loc.file_offset, loc.size);
        if (loc_end <= offset || loc_begin >= end) {
            continue;
        }
        out.push_back(loc);
    }

    std::sort(out.begin(), out.end(), [](const BlockLocation& a, const BlockLocation& b) {
        return a.file_offset < b.file_offset;
    });
    return out;
}

void MetadataStore::rebuild_block_index_locked(const std::string& path) {
    auto file_it = file_map_.find(path);
    if (file_it == file_map_.end()) {
        return;
    }

    for (auto it = block_index_.begin(); it != block_index_.end();) {
        if (it->second.path == path) {
            it = block_index_.erase(it);
        } else {
            ++it;
        }
    }

    for (const auto& loc : file_it->second.extents) {
        block_index_[loc.block_id] = BlockIndexEntry{path, loc.file_offset, loc.size};
    }
}

void MetadataStore::update(const std::string& path,
                           BlockId block_id,
                           TierId tier_id,
                           uint64_t file_offset,
                           uint64_t size) {
    if (size == 0) {
        return;
    }

    std::unique_lock lock(mu_);
    auto& file = file_map_[path];
    const uint64_t new_begin = file_offset;
    const uint64_t new_end = checked_end(file_offset, size);

    std::vector<BlockLocation> next;
    next.reserve(file.extents.size() + 3);

    for (const auto& loc : file.extents) {
        const uint64_t old_begin = loc.file_offset;
        const uint64_t old_end = checked_end(loc.file_offset, loc.size);

        if (old_end <= new_begin || old_begin >= new_end) {
            next.push_back(loc);
            continue;
        }

        if (old_begin < new_begin) {
            next.push_back(BlockLocation{
                extent_allocator_.next(),
                loc.tier_id,
                old_begin,
                new_begin - old_begin,
            });
        }

        if (old_end > new_end) {
            next.push_back(BlockLocation{
                extent_allocator_.next(),
                loc.tier_id,
                new_end,
                old_end - new_end,
            });
        }
    }

    next.push_back(BlockLocation{block_id, tier_id, file_offset, size});
    std::sort(next.begin(), next.end(), [](const BlockLocation& a, const BlockLocation& b) {
        return a.file_offset < b.file_offset;
    });

    file.extents = std::move(next);
    ++file.version;
    rebuild_block_index_locked(path);
}

void MetadataStore::relocate_block(BlockId block_id, TierId new_tier) {
    std::unique_lock lock(mu_);
    auto idx = block_index_.find(block_id);
    if (idx == block_index_.end()) {
        throw IoError("unknown block in relocate_block()");
    }

    auto file_it = file_map_.find(idx->second.path);
    if (file_it == file_map_.end()) {
        throw IoError("corrupt metadata: missing file for block");
    }

    for (auto& loc : file_it->second.extents) {
        if (loc.block_id == block_id) {
            loc.tier_id = new_tier;
            ++file_it->second.version;
            return;
        }
    }

    throw IoError("unknown block in relocate_block()");
}

LocatedBlock MetadataStore::get_block(BlockId block_id) const {
    std::shared_lock lock(mu_);
    auto idx = block_index_.find(block_id);
    if (idx == block_index_.end()) {
        throw IoError("unknown block in get_block()");
    }

    auto file_it = file_map_.find(idx->second.path);
    if (file_it == file_map_.end()) {
        throw IoError("corrupt metadata: missing file for block");
    }

    for (const auto& loc : file_it->second.extents) {
        if (loc.block_id == block_id) {
            return LocatedBlock{idx->second.path, loc};
        }
    }

    throw IoError("unknown block in get_block()");
}

TierId MetadataStore::tier_of(BlockId block_id) const {
    return get_block(block_id).location.tier_id;
}

uint64_t MetadataStore::version_of(const std::string& path) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    if (it == file_map_.end()) {
        return 0;
    }
    return it->second.version;
}

uint64_t MetadataStore::bump_version(const std::string& path) {
    std::unique_lock lock(mu_);
    auto& file = file_map_[path];
    return ++file.version;
}

ConstantPlacementPolicy::ConstantPlacementPolicy(TierId default_tier)
    : default_tier_(default_tier) {}

TierId ConstantPlacementPolicy::choose_tier(const WriteBlock&) {
    return default_tier_;
}

void AsyncMux::PromotionQueue::push(BlockId block_id) {
    bool wake = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (stopping_) {
            return;
        }

        queue_.push(block_id);
        wake = true;
    }

    if (wake) {
        has_item_.set();
    }
}

void AsyncMux::PromotionQueue::stop() noexcept {
    bool wake = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        stopping_ = true;
        wake = true;
    }

    if (wake) {
        has_item_.set();
    }
}

cppcoro::task<std::optional<BlockId>> AsyncMux::PromotionQueue::pop() {
    while (true) {
        {
            std::lock_guard<std::mutex> guard(mu_);
            if (!queue_.empty()) {
                BlockId block_id = queue_.front();
                queue_.pop();
                co_return block_id;
            }

            if (stopping_) {
                co_return std::nullopt;
            }
        }

        co_await has_item_;
    }
}

AsyncMux::AsyncMux(TierRegistry& tiers,
                   MetadataStore& metadata,
                   PlacementPolicy& placement,
                   cppcoro::static_thread_pool& pool,
                   bool auto_migration_enabled,
                   TierId hot_tier_id)
    : tiers_(tiers),
      metadata_(metadata),
      placement_(placement),
      pool_(pool),
      auto_migration_enabled_(auto_migration_enabled),
      hot_tier_id_(hot_tier_id) {
    if (auto_migration_enabled_) {
        bg_scope_.spawn(background_loop());
    }
}

AsyncMux::~AsyncMux() {
    stopping_.store(true, std::memory_order_release);
    promotion_queue_.stop();
    cppcoro::sync_wait(bg_scope_.join());
}

void AsyncMux::enqueue_background_promotion(BlockId block_id) {
    if (!auto_migration_enabled_) {
        return;
    }

    promotion_queue_.push(block_id);
}

cppcoro::task<void> AsyncMux::background_loop() {
    co_await pool_.schedule();

    while (!stopping_.load(std::memory_order_acquire)) {
        auto block_id = co_await promotion_queue_.pop();
        if (!block_id || stopping_.load(std::memory_order_acquire)) {
            co_return;
        }

        try {
            const TierId current = metadata_.tier_of(*block_id);
            if (current != hot_tier_id_) {
                co_await promote(*block_id, hot_tier_id_);
            }
        } catch (...) {
            // Best effort background promotion. Foreground requests remain authoritative.
        }
    }
}

cppcoro::task<IoBuffer> AsyncMux::read(const std::string& path,
                                       uint64_t offset,
                                       uint64_t size) {
    if (size == 0) {
        co_return IoBuffer{};
    }

    auto blocks = metadata_.lookup(path, offset, size);
    std::vector<cppcoro::task<IoBuffer>> tasks;
    std::vector<BlockLocation> overlaps;
    tasks.reserve(blocks.size());
    overlaps.reserve(blocks.size());

    const uint64_t read_end = checked_end(offset, size);
    for (const auto& b : blocks) {
        const uint64_t block_end = checked_end(b.file_offset, b.size);
        const uint64_t overlap_begin = std::max<uint64_t>(b.file_offset, offset);
        const uint64_t overlap_end = std::min<uint64_t>(block_end, read_end);
        if (overlap_begin >= overlap_end) {
            continue;
        }

        const uint64_t overlap_size = overlap_end - overlap_begin;
        const uint64_t tier_offset = overlap_begin;
        Tier& tier = tiers_.get(b.tier_id);
        tasks.emplace_back(tier.read_at(path, tier_offset, overlap_size));
        overlaps.push_back(BlockLocation{b.block_id, b.tier_id, overlap_begin, overlap_size});

        if (b.tier_id != hot_tier_id_) {
            enqueue_background_promotion(b.block_id);
        }
    }

    auto results = co_await cppcoro::when_all(std::move(tasks));
    co_return assemble(overlaps, std::move(results), offset, size);
}

cppcoro::task<void> AsyncMux::write(const std::string& path,
                                    uint64_t offset,
                                    asyncmux::span<const Byte> data) {
    if (data.empty()) {
        co_return;
    }

    auto blocks = split(offset, data);
    std::vector<PreparedWrite> prepared;
    prepared.reserve(blocks.size());

    for (auto& block : blocks) {
        const TierId chosen_tier = placement_.choose_tier(block);
        prepared.push_back(PreparedWrite{std::move(block), chosen_tier});
    }

    std::vector<cppcoro::task<void>> tasks;
    tasks.reserve(prepared.size());
    for (const auto& item : prepared) {
        Tier& tier = tiers_.get(item.tier_id);
        tasks.emplace_back(tier.write_at(
            path,
            item.block.file_offset,
            asyncmux::span<const Byte>(item.block.data.data(), item.block.data.size())));
    }

    co_await cppcoro::when_all(std::move(tasks));

    for (const auto& item : prepared) {
        metadata_.update(path,
                         item.block.block_id,
                         item.tier_id,
                         item.block.file_offset,
                         item.block.data.size());
    }

    co_return;
}

cppcoro::task<void> AsyncMux::migrate(BlockId block_id, TierId src_id, TierId dst_id) {
    constexpr int kMaxAttempts = 4;

    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        const LocatedBlock located = metadata_.get_block(block_id);
        const auto& old = located.location;

        if (old.tier_id != src_id) {
            throw IoError("migrate: source tier does not match metadata");
        }

        const uint64_t start_version = metadata_.version_of(located.path);
        Tier& src_tier = tiers_.get(src_id);
        Tier& dst_tier = tiers_.get(dst_id);

        auto data = co_await src_tier.read_at(located.path, old.file_offset, old.size);
        co_await dst_tier.write_at(located.path,
                                   old.file_offset,
                                   asyncmux::span<const Byte>(data.data.data(), data.data.size()));

        if (metadata_.version_of(located.path) != start_version) {
            continue;
        }

        metadata_.relocate_block(block_id, dst_id);

        try {
            co_await src_tier.punch_hole(located.path, old.file_offset, old.size);
        } catch (...) {
            // Metadata has already switched; punch_hole is best effort cleanup.
        }

        co_return;
    }

    throw IoError("migrate: failed due to concurrent modification");
}

cppcoro::task<void> AsyncMux::promote(BlockId block_id, TierId hot_tier) {
    const TierId current = metadata_.tier_of(block_id);
    if (current == hot_tier) {
        co_return;
    }
    co_await migrate(block_id, current, hot_tier);
}

std::vector<WriteBlock> AsyncMux::split(uint64_t offset, asyncmux::span<const Byte> data) {
    std::vector<WriteBlock> out;
    size_t cursor = 0;

    while (cursor < data.size()) {
        const uint64_t absolute = offset + cursor;
        const size_t in_block = static_cast<size_t>(absolute % kBlockSize);
        const size_t space_left = kBlockSize - in_block;
        const size_t n = std::min<size_t>(space_left, data.size() - cursor);

        std::vector<Byte> chunk(n);
        std::copy_n(data.begin() + static_cast<std::ptrdiff_t>(cursor), n, chunk.begin());

        out.push_back(WriteBlock{
            .block_id = allocator_.next(),
            .file_offset = absolute,
            .data = std::move(chunk),
        });
        cursor += n;
    }

    return out;
}

IoBuffer AsyncMux::assemble(const std::vector<BlockLocation>& locations,
                            std::vector<IoBuffer> pieces,
                            uint64_t read_offset,
                            uint64_t read_size) {
    IoBuffer out(static_cast<size_t>(read_size));

    for (size_t i = 0; i < locations.size(); ++i) {
        const auto& loc = locations[i];
        const auto& src = pieces[i].data;

        const size_t dst_offset = static_cast<size_t>(loc.file_offset - read_offset);
        const size_t n = std::min(static_cast<size_t>(loc.size), src.size());
        if (dst_offset > out.data.size() || n > out.data.size() - dst_offset) {
            throw IoError("assemble: invalid extent bounds");
        }

        std::copy_n(src.begin(), n, out.data.begin() + static_cast<std::ptrdiff_t>(dst_offset));
    }

    return out;
}

FuseFrontend::FuseFrontend(AsyncMux& mux) : mux_(mux) {}

cppcoro::task<IoBuffer> FuseFrontend::on_read(const std::string& path,
                                              uint64_t offset,
                                              uint64_t size) {
    co_return co_await mux_.read(path, offset, size);
}

cppcoro::task<void> FuseFrontend::on_write(const std::string& path,
                                           uint64_t offset,
                                           asyncmux::span<const Byte> data) {
    co_await mux_.write(path, offset, data);
    co_return;
}

} // namespace asyncmux
