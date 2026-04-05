#include "amux/asyncmux.hh"

#include <algorithm>
#include <fstream>

namespace asyncmux {

// SegmentFileTier::SegmentFileTier
//         Creates an append-only segment-backed tier rooted at root_dir.
SegmentFileTier::SegmentFileTier(TierId id,
                                 std::string name,
                                 std::filesystem::path root_dir,
                                 cppcoro::static_thread_pool& pool,
                                 uint64_t segment_size)
    : id_(id),
      name_(std::move(name)),
      root_dir_(std::move(root_dir)),
      pool_(pool),
      segment_size_(segment_size) {
    std::filesystem::create_directories(root_dir_);
}

// SegmentFileTier::id
//         Returns this tier's unique identifier.
TierId SegmentFileTier::id() const {
    return id_;
}

// SegmentFileTier::name
//         Returns this tier's display name.
std::string SegmentFileTier::name() const {
    return name_;
}

// SegmentFileTier::segment_path
//         Maps a segment id to a file path under the tier root.
std::filesystem::path SegmentFileTier::segment_path(uint64_t segment_id) const {
    return root_dir_ / ("segment_" + std::to_string(segment_id) + ".bin");
}

// SegmentFileTier::allocate_extent
//         Appends a new extent and returns its segment id and offset.
std::pair<uint64_t, uint64_t> SegmentFileTier::allocate_extent(uint64_t size) {
    std::lock_guard lock(mu_);
    if (size > segment_size_) {
        throw IoError("allocate_extent: extent larger than segment size");
    }

    if (current_segment_offset_ + size > segment_size_) {
        ++current_segment_id_;
        current_segment_offset_ = 0;
    }

    const std::pair<uint64_t, uint64_t> out{current_segment_id_, current_segment_offset_};
    current_segment_offset_ += size;
    return out;
}

// SegmentFileTier::read_extent
//         Reads a byte extent from a segment file.
cppcoro::task<IoBuffer> SegmentFileTier::read_extent(uint64_t segment_id,
                                                     uint64_t tier_offset,
                                                     uint64_t size) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    const auto path = segment_path(segment_id);

    if (!std::filesystem::exists(path)) {
        throw IoError("read_extent: missing segment file");
    }

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw IoError("read_extent: failed to open segment file");
    }

    std::vector<Byte> out(static_cast<std::size_t>(size));
    in.seekg(static_cast<std::streamoff>(tier_offset), std::ios::beg);
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));

    if (!in && !in.eof()) {
        throw IoError("read_extent: failed during read");
    }

    co_return IoBuffer{std::move(out)};
}

// SegmentFileTier::write_extent
//         Writes bytes to a byte extent in a segment file.
cppcoro::task<void> SegmentFileTier::write_extent(uint64_t segment_id,
                                                  uint64_t tier_offset,
                                                  std::span<const Byte> data) {
    co_await pool_.schedule();

    std::lock_guard lock(mu_);
    std::filesystem::create_directories(root_dir_);
    const auto path = segment_path(segment_id);

    std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!io) {
        std::ofstream create(path, std::ios::binary);
        if (!create) {
            throw IoError("write_extent: failed to create segment file");
        }
        create.close();
        io.open(path, std::ios::binary | std::ios::in | std::ios::out);
    }

    if (!io) {
        throw IoError("write_extent: failed to open segment file for read/write");
    }

    io.seekp(static_cast<std::streamoff>(tier_offset), std::ios::beg);
    io.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));

    if (!io) {
        throw IoError("write_extent: failed during write");
    }

    co_return;
}

// SegmentFileTier::read_block
//         Temporary compatibility shim for the old block API.
cppcoro::task<IoBuffer> SegmentFileTier::read_block(BlockId,
                                                    uint64_t,
                                                    uint64_t) {
    throw IoError("block-based API unsupported for SegmentFileTier");
}

// SegmentFileTier::write_block
//         Temporary compatibility shim for the old block API.
cppcoro::task<void> SegmentFileTier::write_block(BlockId,
                                                 uint64_t,
                                                 std::span<const Byte>) {
    throw IoError("block-based API unsupported for SegmentFileTier");
}

// SegmentFileTier::delete_block
//         Temporary compatibility shim for the old block API.
cppcoro::task<void> SegmentFileTier::delete_block(BlockId) {
    throw IoError("block-based API unsupported for SegmentFileTier");
}

} // namespace asyncmux