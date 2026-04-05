#include "amux/asyncmux.hh"

#include <fstream>

namespace asyncmux {

// DiskTier::DiskTier
//         Creates a filesystem-backed tier rooted under root_dir.
DiskTier::DiskTier(TierId id,
                   std::string name,
                   std::filesystem::path root_dir,
                   cppcoro::static_thread_pool& pool)
    : id_(id),
      name_(std::move(name)),
      root_dir_(std::move(root_dir)),
      pool_(pool) {
    std::filesystem::create_directories(root_dir_);
}

// DiskTier::id
//         Returns this tier's unique identifier.
TierId DiskTier::id() const {
    return id_;
}

// DiskTier::name
//         Returns this tier's display name.
std::string DiskTier::name() const {
    return name_;
}

// DiskTier::block_path
//         Maps a block id to a file path under the tier root.
std::filesystem::path DiskTier::block_path(BlockId block_id) const {
    return root_dir_ / ("block_" + std::to_string(block_id) + ".bin");
}

// DiskTier::read_block
//         Reads a block slice from a file under the tier root.
cppcoro::task<IoBuffer> DiskTier::read_block(BlockId block_id,
                                             uint64_t offset,
                                             uint64_t size) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = block_path(block_id);

    if (!std::filesystem::exists(path)) {
        throw IoError("disk read_block: missing block");
    }

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw IoError("disk read_block: failed to open block file");
    }

    const auto file_size = std::filesystem::file_size(path);
    if (offset > file_size) {
        throw IoError("disk read_block: offset out of range");
    }

    const size_t n =
        static_cast<size_t>(std::min<uint64_t>(size, file_size - offset));
    std::vector<Byte> out(n);

    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(n));
    if (!in && !in.eof()) {
        throw IoError("disk read_block: failed during read");
    }

    co_return IoBuffer{std::move(out)};
}

// DiskTier::write_block
//         Writes bytes into a block file under the tier root.
cppcoro::task<void> DiskTier::write_block(BlockId block_id,
                                          uint64_t offset,
                                          std::span<const Byte> data) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    std::filesystem::create_directories(root_dir_);
    const auto path = block_path(block_id);

    std::vector<Byte> existing;
    if (std::filesystem::exists(path)) {
        const auto old_size = std::filesystem::file_size(path);
        existing.resize(static_cast<size_t>(old_size));

        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw IoError("disk write_block: failed to open existing block");
        }

        in.read(reinterpret_cast<char*>(existing.data()),
                static_cast<std::streamsize>(existing.size()));
        if (!in && !in.eof()) {
            throw IoError("disk write_block: failed reading existing block");
        }
    }

    const size_t needed = static_cast<size_t>(offset) + data.size();
    if (existing.size() < needed) {
        existing.resize(needed);
    }

    std::copy(data.begin(), data.end(), existing.begin() + offset);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw IoError("disk write_block: failed to open block for write");
    }

    out.write(reinterpret_cast<const char*>(existing.data()),
              static_cast<std::streamsize>(existing.size()));
    if (!out) {
        throw IoError("disk write_block: failed during write");
    }

    co_return;
}

// DiskTier::delete_block
//         Removes a block file from the tier root.
cppcoro::task<void> DiskTier::delete_block(BlockId block_id) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = block_path(block_id);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        throw IoError("disk delete_block: failed to remove block file");
    }

    co_return;
}

} // namespace asyncmux
