#include "blockmux.hh"

#include <cerrno>

namespace bmux {

IoBuffer::IoBuffer(std::size_t n) : data(n) {}

IoBuffer::IoBuffer(std::vector<std::byte> d) : data(std::move(d)) {}

std::size_t IoBuffer::size() const {
    return data.size();
}

std::byte* IoBuffer::bytes() {
    return data.data();
}

const std::byte* IoBuffer::bytes() const {
    return data.data();
}

BlockId BlockAllocator::next() {
    return next_id_.fetch_add(1, std::memory_order_relaxed);
}

ThreadPool::ThreadPool(std::size_t nthreads) {
    for (std::size_t i = 0; i < nthreads; ++i) {
        workers_.emplace_back([this] {
            for (;;) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(mu_);
                    cv_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
                    if (stop_ && jobs_.empty()) {
                        return;
                    }
                    job = std::move(jobs_.front());
                    jobs_.pop();
                }
                job();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void ThreadPool::submit(std::function<void()> job) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        jobs_.push(std::move(job));
    }
    cv_.notify_one();
}

void TierRegistry::add(std::unique_ptr<Tier> tier) {
    auto [it, ok] = tiers_.emplace(tier->id(), std::move(tier));
    if (!ok) {
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

FileSystemTier::FileSystemTier(TierId id,
                               std::string name,
                               std::filesystem::path root_dir)
    : id_(id), name_(std::move(name)), root_dir_(std::move(root_dir)) {
    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        throw IoError("failed to create tier root");
    }
}

TierId FileSystemTier::id() const {
    return id_;
}

std::string FileSystemTier::name() const {
    return name_;
}

IoBuffer FileSystemTier::read_at(const std::string& relative_path,
                                 std::uint64_t offset,
                                 std::uint64_t size) {
    const auto path = full_path(relative_path);
    const auto lk = lock_for_path(path);
    std::shared_lock<std::shared_mutex> guard(*lk);

    std::vector<std::byte> out(static_cast<std::size_t>(size), std::byte{0});
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return IoBuffer(std::move(out));
        }
        throw IoError("read_at open failed");
    }

    ssize_t n = ::pread(fd, out.data(), size, static_cast<off_t>(offset));
    int saved = errno;
    ::close(fd);
    if (n < 0) {
        throw IoError(std::string("pread failed: ") + std::strerror(saved));
    }
    return IoBuffer(std::move(out));
}

void FileSystemTier::write_at(const std::string& relative_path,
                              std::uint64_t offset,
                              span<const std::byte> data) {
    const auto path = full_path(relative_path);
    ensure_parent_dirs(path);
    const auto lk = lock_for_path(path);
    std::unique_lock<std::shared_mutex> guard(*lk);

    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        throw IoError("write_at open failed");
    }

    const std::byte* ptr = data.data();
    std::size_t rem = data.size();
    off_t pos = static_cast<off_t>(offset);
    while (rem > 0) {
        ssize_t n = ::pwrite(fd, ptr, rem, pos);
        int saved = errno;
        if (n < 0) {
            ::close(fd);
            throw IoError(std::string("pwrite failed: ") + std::strerror(saved));
        }
        ptr += n;
        rem -= static_cast<std::size_t>(n);
        pos += n;
    }
    ::close(fd);
}

void FileSystemTier::punch_hole(const std::string& relative_path,
                                std::uint64_t offset,
                                std::uint64_t size) {
    if (size == 0) {
        return;
    }
    const auto path = full_path(relative_path);
    const auto lk = lock_for_path(path);
    std::unique_lock<std::shared_mutex> guard(*lk);

    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        if (errno == ENOENT) {
            return;
        }
        throw IoError("punch_hole open failed");
    }
#ifdef __linux__
    if (::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                    static_cast<off_t>(offset), static_cast<off_t>(size)) != 0) {
        if (errno != EOPNOTSUPP && errno != ENOSYS) {
            int saved = errno;
            ::close(fd);
            throw IoError(std::string("fallocate failed: ") + std::strerror(saved));
        }
    }
#endif
    ::close(fd);
}

void FileSystemTier::remove_file(const std::string& relative_path) {
    const auto path = full_path(relative_path);
    const auto lk = lock_for_path(path);
    std::unique_lock<std::shared_mutex> guard(*lk);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        throw IoError("remove_file failed");
    }
}

std::filesystem::path FileSystemTier::full_path(const std::string& relative_path) const {
    std::filesystem::path rel(relative_path);
    if (rel.is_absolute()) {
        rel = rel.lexically_relative("/");
    }
    return root_dir_ / rel;
}

void FileSystemTier::ensure_parent_dirs(const std::filesystem::path& p) const {
    auto parent = p.parent_path();
    if (parent.empty()) {
        return;
    }
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        throw IoError("create_directories failed");
    }
}

std::shared_ptr<std::shared_mutex> FileSystemTier::lock_for_path(const std::filesystem::path& p) const {
    const std::string key = p.lexically_normal().string();
    std::lock_guard<std::mutex> g(lock_mu_);
    auto& e = file_locks_[key];
    if (!e) {
        e = std::make_shared<std::shared_mutex>();
    }
    return e;
}

std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
                                                 std::uint64_t offset,
                                                 std::uint64_t size) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    if (it == file_map_.end() || size == 0) {
        return {};
    }

    std::vector<BlockLocation> out;
    const auto end = checked_end(offset, size);
    for (const auto& loc : it->second.extents) {
        const auto lb = loc.file_offset;
        const auto le = checked_end(loc.file_offset, loc.size);
        if (le <= offset || lb >= end) {
            continue;
        }
        out.push_back(loc);
    }

    std::sort(out.begin(), out.end(),
              [](const auto& a, const auto& b) { return a.file_offset < b.file_offset; });
    return out;
}

void MetadataStore::update(const std::string& path,
                           BlockId block_id,
                           TierId tier_id,
                           std::uint64_t file_offset,
                           std::uint64_t size) {
    if (size == 0) {
        return;
    }

    std::unique_lock lock(mu_);
    auto& file = file_map_[path];
    const auto new_begin = file_offset;
    const auto new_end = checked_end(file_offset, size);

    std::vector<BlockLocation> next;
    next.reserve(file.extents.size() + 3);

    for (const auto& loc : file.extents) {
        const auto old_begin = loc.file_offset;
        const auto old_end = checked_end(loc.file_offset, loc.size);
        if (old_end <= new_begin || old_begin >= new_end) {
            next.push_back(loc);
            continue;
        }
        if (old_begin < new_begin) {
            next.push_back({extent_alloc_.next(), loc.tier_id, old_begin, new_begin - old_begin});
        }
        if (old_end > new_end) {
            next.push_back({extent_alloc_.next(), loc.tier_id, new_end, old_end - new_end});
        }
    }

    next.push_back({block_id, tier_id, file_offset, size});
    std::sort(next.begin(), next.end(),
              [](const auto& a, const auto& b) { return a.file_offset < b.file_offset; });
    file.extents = std::move(next);
    ++file.version;
    rebuild_block_index_locked(path);
}

void MetadataStore::relocate_block(BlockId block_id, TierId new_tier) {
    std::unique_lock lock(mu_);
    auto idx = block_index_.find(block_id);
    if (idx == block_index_.end()) {
        throw IoError("unknown block");
    }
    auto fit = file_map_.find(idx->second.path);
    if (fit == file_map_.end()) {
        throw IoError("corrupt metadata");
    }
    for (auto& loc : fit->second.extents) {
        if (loc.block_id == block_id) {
            loc.tier_id = new_tier;
            ++fit->second.version;
            return;
        }
    }
    throw IoError("unknown block");
}

LocatedBlock MetadataStore::get_block(BlockId block_id) const {
    std::shared_lock lock(mu_);
    auto idx = block_index_.find(block_id);
    if (idx == block_index_.end()) {
        throw IoError("unknown block");
    }
    auto fit = file_map_.find(idx->second.path);
    if (fit == file_map_.end()) {
        throw IoError("corrupt metadata");
    }
    for (const auto& loc : fit->second.extents) {
        if (loc.block_id == block_id) {
            return {idx->second.path, loc};
        }
    }
    throw IoError("unknown block");
}

TierId MetadataStore::tier_of(BlockId block_id) const {
    return get_block(block_id).location.tier_id;
}

std::uint64_t MetadataStore::version_of(const std::string& path) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    return it == file_map_.end() ? 0 : it->second.version;
}

std::uint64_t MetadataStore::checked_end(std::uint64_t off, std::uint64_t sz) {
    return (sz > UINT64_MAX - off) ? UINT64_MAX : off + sz;
}

void MetadataStore::rebuild_block_index_locked(const std::string& path) {
    for (auto it = block_index_.begin(); it != block_index_.end();) {
        if (it->second.path == path) {
            it = block_index_.erase(it);
        } else {
            ++it;
        }
    }

    auto fit = file_map_.find(path);
    if (fit == file_map_.end()) {
        return;
    }
    for (const auto& loc : fit->second.extents) {
        block_index_[loc.block_id] = {path, loc.file_offset, loc.size};
    }
}

MutablePlacementPolicy::MutablePlacementPolicy(TierId initial) : tier_(initial) {}

void MutablePlacementPolicy::set(TierId tier) {
    tier_ = tier;
}

TierId MutablePlacementPolicy::choose_tier(const WriteBlock&) {
    return tier_;
}

BlockingMux::BlockingMux(TierRegistry& tiers,
                         MetadataStore& metadata,
                         PlacementPolicy& placement,
                         std::size_t worker_threads,
                         bool auto_migration_enabled,
                         TierId hot_tier_id)
    : tiers_(tiers),
      metadata_(metadata),
      placement_(placement),
      pool_(worker_threads),
      auto_migration_enabled_(auto_migration_enabled),
      hot_tier_id_(hot_tier_id) {
    if (auto_migration_enabled_) {
        bg_thread_ = std::thread([this] { background_loop(); });
    }
}

BlockingMux::~BlockingMux() {
    {
        std::lock_guard<std::mutex> g(bg_mu_);
        bg_stop_ = true;
    }
    bg_cv_.notify_all();
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
}

IoBuffer BlockingMux::read(const std::string& raw_path,
                           std::uint64_t offset,
                           std::uint64_t size) {
    if (size == 0) {
        return IoBuffer{};
    }

    const std::string path = normalize_path(raw_path);
    auto blocks = metadata_.lookup(path, offset, size);

    struct Shared {
        std::mutex mu;
        std::condition_variable cv;
        std::size_t done = 0;
        std::exception_ptr ex;
    } shared;

    std::vector<IoBuffer> pieces(blocks.size());
    std::vector<BlockLocation> overlaps(blocks.size());

    std::size_t jobs = 0;
    const auto read_end = checked_end(offset, size);

    for (std::size_t i = 0; i < blocks.size(); ++i) {
        const auto& b = blocks[i];
        const auto block_end = checked_end(b.file_offset, b.size);
        const auto ob = std::max<std::uint64_t>(b.file_offset, offset);
        const auto oe = std::min<std::uint64_t>(block_end, read_end);
        if (ob >= oe) {
            continue;
        }

        overlaps[i] = {b.block_id, b.tier_id, ob, oe - ob};
        ++jobs;

        pool_.submit([&, i, path, ob, oe] {
            try {
                pieces[i] = tiers_.get(blocks[i].tier_id).read_at(path, ob, oe - ob);
                if (blocks[i].tier_id != hot_tier_id_) {
                    enqueue_promotion(blocks[i].block_id);
                }
            } catch (...) {
                std::lock_guard<std::mutex> g(shared.mu);
                if (!shared.ex) {
                    shared.ex = std::current_exception();
                }
            }
            {
                std::lock_guard<std::mutex> g(shared.mu);
                ++shared.done;
            }
            shared.cv.notify_one();
        });
    }

    {
        std::unique_lock<std::mutex> lock(shared.mu);
        shared.cv.wait(lock, [&] { return shared.done == jobs; });
    }

    if (shared.ex) {
        std::rethrow_exception(shared.ex);
    }

    std::vector<IoBuffer> used_pieces;
    std::vector<BlockLocation> used_locs;
    for (std::size_t i = 0; i < overlaps.size(); ++i) {
        if (overlaps[i].size == 0) {
            continue;
        }
        used_pieces.push_back(std::move(pieces[i]));
        used_locs.push_back(overlaps[i]);
    }

    return assemble(used_locs, used_pieces, offset, size);
}

void BlockingMux::write(const std::string& raw_path,
                        std::uint64_t offset,
                        span<const std::byte> data) {
    if (data.empty()) {
        return;
    }

    const std::string path = normalize_path(raw_path);
    auto blocks = split(offset, data);

    struct PreparedWrite {
        WriteBlock block;
        TierId tier_id;
    };

    std::vector<PreparedWrite> prepared;
    prepared.reserve(blocks.size());
    for (auto& b : blocks) {
        prepared.push_back({std::move(b), placement_.choose_tier(b)});
    }

    struct Shared {
        std::mutex mu;
        std::condition_variable cv;
        std::size_t done = 0;
        std::exception_ptr ex;
    } shared;

    for (std::size_t i = 0; i < prepared.size(); ++i) {
        pool_.submit([&, i, path] {
            const auto& item = prepared[i];
            try {
                tiers_.get(item.tier_id).write_at(
                    path,
                    item.block.file_offset,
                    span<const std::byte>(item.block.data.data(), item.block.data.size()));
            } catch (...) {
                std::lock_guard<std::mutex> g(shared.mu);
                if (!shared.ex) {
                    shared.ex = std::current_exception();
                }
            }
            {
                std::lock_guard<std::mutex> g(shared.mu);
                ++shared.done;
            }
            shared.cv.notify_one();
        });
    }

    {
        std::unique_lock<std::mutex> lock(shared.mu);
        shared.cv.wait(lock, [&] { return shared.done == prepared.size(); });
    }

    if (shared.ex) {
        std::rethrow_exception(shared.ex);
    }

    for (const auto& item : prepared) {
        metadata_.update(path,
                         item.block.block_id,
                         item.tier_id,
                         item.block.file_offset,
                         item.block.data.size());
    }
}

void BlockingMux::migrate(BlockId block_id, TierId src_id, TierId dst_id) {
    constexpr int kMaxAttempts = 4;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        const auto located = metadata_.get_block(block_id);
        const auto& old = located.location;

        if (old.tier_id == dst_id) {
            return;
        }
        if (old.tier_id != src_id) {
            throw IoError("migrate: source tier mismatch");
        }

        const auto start_version = metadata_.version_of(located.path);
        auto data = tiers_.get(src_id).read_at(located.path, old.file_offset, old.size);
        tiers_.get(dst_id).write_at(
            located.path,
            old.file_offset,
            span<const std::byte>(data.data.data(), data.data.size()));

        if (metadata_.version_of(located.path) != start_version) {
            continue;
        }

        metadata_.relocate_block(block_id, dst_id);
        try {
            tiers_.get(src_id).punch_hole(located.path, old.file_offset, old.size);
        } catch (...) {
        }
        return;
    }

    throw IoError("migrate: concurrent modification");
}

void BlockingMux::promote(BlockId block_id, TierId hot_tier) {
    const TierId cur = metadata_.tier_of(block_id);
    if (cur == hot_tier) {
        return;
    }
    migrate(block_id, cur, hot_tier);
}

std::uint64_t BlockingMux::checked_end(std::uint64_t off, std::uint64_t sz) {
    return (sz > UINT64_MAX - off) ? UINT64_MAX : off + sz;
}

std::string BlockingMux::normalize_path(const std::string& path) {
    std::filesystem::path p(path);
    p = p.lexically_normal();
    if (p.is_absolute()) {
        p = p.lexically_relative("/");
    }
    return p.string();
}

void BlockingMux::enqueue_promotion(BlockId block_id) {
    if (!auto_migration_enabled_) {
        return;
    }
    std::lock_guard<std::mutex> g(bg_mu_);
    if (!bg_queued_.insert(block_id).second) {
        return;
    }
    bg_queue_.push(block_id);
    bg_cv_.notify_one();
}

void BlockingMux::background_loop() {
    for (;;) {
        BlockId block_id = 0;
        {
            std::unique_lock<std::mutex> lock(bg_mu_);
            bg_cv_.wait(lock, [this] { return bg_stop_ || !bg_queue_.empty(); });
            if (bg_stop_ && bg_queue_.empty()) {
                return;
            }
            block_id = bg_queue_.front();
            bg_queue_.pop();
            bg_queued_.erase(block_id);
        }

        try {
            const TierId cur = metadata_.tier_of(block_id);
            if (cur != hot_tier_id_) {
                promote(block_id, hot_tier_id_);
            }
        } catch (...) {
        }
    }
}

std::vector<WriteBlock> BlockingMux::split(std::uint64_t offset, span<const std::byte> data) {
    std::vector<WriteBlock> out;
    std::size_t cursor = 0;
    while (cursor < data.size()) {
        const auto absolute = offset + cursor;
        const auto in_block = static_cast<std::size_t>(absolute % kBlockSize);
        const auto space_left = kBlockSize - in_block;
        const auto n = std::min<std::size_t>(space_left, data.size() - cursor);
        std::vector<std::byte> chunk(n);
        std::copy_n(data.begin() + static_cast<std::ptrdiff_t>(cursor), n, chunk.begin());
        out.push_back({allocator_.next(), absolute, std::move(chunk)});
        cursor += n;
    }
    return out;
}

IoBuffer BlockingMux::assemble(const std::vector<BlockLocation>& locations,
                               const std::vector<IoBuffer>& pieces,
                               std::uint64_t read_offset,
                               std::uint64_t read_size) {
    IoBuffer out(static_cast<std::size_t>(read_size));
    for (std::size_t i = 0; i < locations.size(); ++i) {
        const auto& loc = locations[i];
        const auto& src = pieces[i].data;
        const auto dst_offset = static_cast<std::size_t>(loc.file_offset - read_offset);
        const auto n = std::min<std::size_t>(static_cast<std::size_t>(loc.size), src.size());
        if (dst_offset > out.data.size() || n > out.data.size() - dst_offset) {
            throw IoError("assemble bounds");
        }
        std::copy_n(src.begin(), n, out.data.begin() + static_cast<std::ptrdiff_t>(dst_offset));
    }
    return out;
}

} // namespace blockingmux