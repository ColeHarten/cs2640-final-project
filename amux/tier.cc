#include "tier.hh"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <unistd.h>

namespace asyncmux {

namespace {

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

TierId FileSystemTier::id() const {
    return id_;
}

std::string FileSystemTier::name() const {
    return name_;
}

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

    if (!std::filesystem::exists(path)) {
        co_return IoBuffer{std::move(out)};
    }

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        const int saved_errno = errno;
        throw IoError(io_error_with_context("read_at: failed to open file",
                                            path,
                                            saved_errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    in.seekg(0, std::ios::end);
    const auto end_pos = in.tellg();
    if (end_pos < 0) {
        const int saved_errno = errno;
        throw IoError(io_error_with_context("read_at: failed to determine file size",
                                            path,
                                            saved_errno,
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
        const int saved_errno = errno;
        throw IoError(io_error_with_context("read_at: failed during read",
                                            path,
                                            saved_errno,
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

    std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!io) {
        const int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
        if (fd < 0) {
            const int saved_errno = errno;
            throw IoError(io_error_with_context("write_at: failed to create file",
                                                path,
                                                saved_errno,
                                                __FILE__,
                                                __LINE__,
                                                __func__));
        }
        ::close(fd);
        io.open(path, std::ios::binary | std::ios::in | std::ios::out);
    }

    if (!io) {
        const int saved_errno = errno;
        throw IoError(io_error_with_context("write_at: failed to open file",
                                            path,
                                            saved_errno,
                                            __FILE__,
                                            __LINE__,
                                            __func__));
    }

    io.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    io.write(reinterpret_cast<const char*>(data.data()),
             static_cast<std::streamsize>(data.size()));
    io.flush();

    if (!io) {
        const int saved_errno = errno;
        throw IoError(io_error_with_context("write_at: failed during write",
                                            path,
                                            saved_errno,
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

} // namespace asyncmux