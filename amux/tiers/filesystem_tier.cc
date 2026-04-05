#include "amux/asyncmux.hh"

#include <algorithm>
#include <fstream>

namespace asyncmux {

// FileSystemTier::FileSystemTier
FileSystemTier::FileSystemTier(TierId id,
                               std::string name,
                               std::filesystem::path root_dir,
                               cppcoro::static_thread_pool& pool)
    : id_(id),
      name_(std::move(name)),
      root_dir_(std::move(root_dir)),
      pool_(pool) {
    std::filesystem::create_directories(root_dir_);
}

// FileSystemTier::id
TierId FileSystemTier::id() const {
    return id_;
}

// FileSystemTier::name
std::string FileSystemTier::name() const {
    return name_;
}

// FileSystemTier::full_path
std::filesystem::path FileSystemTier::full_path(const std::string& relative_path) const {
    std::filesystem::path rel(relative_path);
    if (rel.is_absolute()) {
        rel = rel.lexically_relative("/");
    }
    return root_dir_ / rel;
}

// FileSystemTier::ensure_parent_dirs
void FileSystemTier::ensure_parent_dirs(const std::filesystem::path& p) const {
    const auto parent = p.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
}

// FileSystemTier::read_at
cppcoro::task<IoBuffer> FileSystemTier::read_at(const std::string& relative_path,
                                                uint64_t offset,
                                                uint64_t size) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = full_path(relative_path);

    std::vector<Byte> out(static_cast<size_t>(size), Byte{0});

    if (!std::filesystem::exists(path)) {
        co_return IoBuffer{std::move(out)};
    }

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw IoError("read_at: failed to open file");
    }

    in.seekg(0, std::ios::end);
    const auto file_size = static_cast<uint64_t>(in.tellg());

    if (offset >= file_size) {
        co_return IoBuffer{std::move(out)};
    }

    const uint64_t readable = std::min<uint64_t>(size, file_size - offset);
    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(readable));

    if (!in && !in.eof()) {
        throw IoError("read_at: failed during read");
    }

    co_return IoBuffer{std::move(out)};
}

// FileSystemTier::write_at
cppcoro::task<void> FileSystemTier::write_at(const std::string& relative_path,
                                             uint64_t offset,
                                             std::span<const Byte> data) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = full_path(relative_path);
    ensure_parent_dirs(path);

    std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!io) {
        std::ofstream create(path, std::ios::binary);
        if (!create) {
            throw IoError("write_at: failed to create file");
        }
        create.close();
        io.open(path, std::ios::binary | std::ios::in | std::ios::out);
    }

    if (!io) {
        throw IoError("write_at: failed to open file");
    }

    io.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    io.write(reinterpret_cast<const char*>(data.data()),
             static_cast<std::streamsize>(data.size()));

    if (!io) {
        throw IoError("write_at: failed during write");
    }

    co_return;
}

// FileSystemTier::remove_file
cppcoro::task<void> FileSystemTier::remove_file(const std::string& relative_path) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = full_path(relative_path);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        throw IoError("remove_file: failed to remove file");
    }

    co_return;
}

} // namespace asyncmux