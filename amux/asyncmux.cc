#include "asyncmux.hh"

#include <algorithm>
#include <fstream>

namespace asyncmux {

// IoBuffer::IoBuffer
IoBuffer::IoBuffer() = default;

// IoBuffer::IoBuffer
IoBuffer::IoBuffer(size_t n) : data(n) {}

// IoBuffer::IoBuffer
IoBuffer::IoBuffer(std::vector<Byte> bytes) : data(std::move(bytes)) {}

// IoBuffer::size
size_t IoBuffer::size() const {
    return data.size();
}

// IoBuffer::bytes
Byte* IoBuffer::bytes() {
    return data.data();
}

// IoBuffer::bytes
const Byte* IoBuffer::bytes() const {
    return data.data();
}

// MetadataStore::lookup
std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
                                                 uint64_t offset,
                                                 uint64_t size) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    if (it == file_map_.end()) {
        return {};
    }

    std::vector<BlockLocation> out;
    const uint64_t end = offset + size;
    for (const auto& loc : it->second) {
        const uint64_t loc_begin = loc.file_offset;
        const uint64_t loc_end = loc.file_offset + loc.size;
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

// MetadataStore::update
void MetadataStore::update(const std::string& path,
                           BlockId block_id,
                           TierId tier_id,
                           uint64_t file_offset,
                           uint64_t size) {
    std::unique_lock lock(mu_);
    auto& vec = file_map_[path];
    auto it = std::find_if(vec.begin(), vec.end(), [&](const BlockLocation& loc) {
        return loc.block_id == block_id;
    });

    if (it == vec.end()) {
        vec.push_back(BlockLocation{block_id, tier_id, file_offset, size});
    } else {
        it->tier_id = tier_id;
        it->file_offset = file_offset;
        it->size = size;
    }
}

// MetadataStore::update_block
void MetadataStore::update_block(BlockId block_id, TierId new_tier) {
    std::unique_lock lock(mu_);
    for (auto& [_, vec] : file_map_) {
        for (auto& loc : vec) {
            if (loc.block_id == block_id) {
                loc.tier_id = new_tier;
                return;
            }
        }
    }
    throw IoError("unknown block in update_block()");
}

// MetadataStore::relocate_block
void MetadataStore::relocate_block(BlockId block_id, TierId new_tier) {
    std::unique_lock lock(mu_);
    for (auto& [_, vec] : file_map_) {
        for (auto& loc : vec) {
            if (loc.block_id == block_id) {
                loc.tier_id = new_tier;
                return;
            }
        }
    }
    throw IoError("unknown block in relocate_block()");
}

// MetadataStore::get_block
LocatedBlock MetadataStore::get_block(BlockId block_id) const {
    std::shared_lock lock(mu_);
    for (const auto& [path, vec] : file_map_) {
        for (const auto& loc : vec) {
            if (loc.block_id == block_id) {
                return LocatedBlock{path, loc};
            }
        }
    }
    throw IoError("unknown block in get_block()");
}

// MetadataStore::tier_of
TierId MetadataStore::tier_of(BlockId block_id) const {
    std::shared_lock lock(mu_);
    for (const auto& [_, vec] : file_map_) {
        for (const auto& loc : vec) {
            if (loc.block_id == block_id) {
                return loc.tier_id;
            }
        }
    }
    throw IoError("unknown block in tier_of()");
}


// TierRegistry::add
void TierRegistry::add(std::unique_ptr<Tier> tier) {
    auto [_, inserted] = tiers_.emplace(tier->id(), std::move(tier));
    if (!inserted) {
        throw IoError("duplicate tier id");
    }
}

// TierRegistry::get
Tier& TierRegistry::get(TierId id) {
    auto it = tiers_.find(id);
    if (it == tiers_.end()) {
        throw IoError("unknown tier");
    }
    return *it->second;
}

// TierRegistry::get
const Tier& TierRegistry::get(TierId id) const {
    auto it = tiers_.find(id);
    if (it == tiers_.end()) {
        throw IoError("unknown tier");
    }
    return *it->second;
}

// ConstantPlacementPolicy::ConstantPlacementPolicy
ConstantPlacementPolicy::ConstantPlacementPolicy(TierId default_tier)
    : default_tier_(default_tier) {}

// ConstantPlacementPolicy::choose_tier
TierId ConstantPlacementPolicy::choose_tier(const WriteBlock&) {
    return default_tier_;
}

// BlockAllocator::next
BlockId BlockAllocator::next() {
    return next_id_++;
}

// AsyncMux::AsyncMux
AsyncMux::AsyncMux(TierRegistry& tiers,
                   MetadataStore& metadata,
                   PlacementPolicy& placement,
                   cppcoro::static_thread_pool& pool,
                   bool auto_migration_enabled)
    : tiers_(tiers),
      metadata_(metadata),
      placement_(placement),
      pool_(pool),
      auto_migration_enabled_(auto_migration_enabled) {}

// AsyncMux::read
cppcoro::task<IoBuffer> AsyncMux::read(const std::string& path,
                                       uint64_t offset,
                                       uint64_t size) {
    auto blocks = metadata_.lookup(path, offset, size);
    std::vector<cppcoro::task<IoBuffer>> tasks;
    std::vector<BlockLocation> overlaps;
    tasks.reserve(blocks.size());
    overlaps.reserve(blocks.size());

    for (const auto& b : blocks) {
        const uint64_t overlap_begin = std::max<uint64_t>(b.file_offset, offset);
        const uint64_t overlap_end = std::min<uint64_t>(b.file_offset + b.size, offset + size);
        if (overlap_begin >= overlap_end) {
            continue;
        }

        const uint64_t overlap_size = overlap_end - overlap_begin;
        Tier& tier = tiers_.get(b.tier_id);
        tasks.emplace_back(tier.read_at(path, overlap_begin, overlap_size));
        overlaps.push_back(BlockLocation{
            b.block_id,
            b.tier_id,
            overlap_begin,
            overlap_size
        });
    }

    auto results = co_await cppcoro::when_all(std::move(tasks));
    co_return assemble(overlaps, std::move(results), offset, size);
}

// AsyncMux::write
cppcoro::task<void> AsyncMux::write(const std::string& path,
                                    uint64_t offset,
                                    std::span<const Byte> data) {
    auto blocks = split(offset, data);

    for (auto& block : blocks) {
        const TierId tier_id = placement_.choose_tier(block);
        Tier& tier = tiers_.get(tier_id);

        co_await tier.write_at(path,
                               block.file_offset,
                               std::span<const Byte>(block.data.data(), block.data.size()));

        metadata_.update(path,
                         block.block_id,
                         tier_id,
                         block.file_offset,
                         block.data.size());
    }

    co_return;
}

// AsyncMux::migrate
cppcoro::task<void> AsyncMux::migrate(BlockId block_id, TierId src_id, TierId dst_id) {
    const LocatedBlock located = metadata_.get_block(block_id);
    const auto& old = located.location;

    if (old.tier_id != src_id) {
        throw IoError("migrate: source tier does not match metadata");
    }

    Tier& src_tier = tiers_.get(src_id);
    Tier& dst_tier = tiers_.get(dst_id);

    auto data = co_await src_tier.read_at(located.path, old.file_offset, old.size);
    co_await dst_tier.write_at(located.path,
                               old.file_offset,
                               std::span<const Byte>(data.data.data(), data.data.size()));

    metadata_.relocate_block(block_id, dst_id);
    co_return;
}

// AsyncMux::promote
cppcoro::task<void> AsyncMux::promote(BlockId block_id, TierId hot_tier) {
    const TierId current = metadata_.tier_of(block_id);
    if (current == hot_tier) {
        co_return;
    }
    co_await migrate(block_id, current, hot_tier);
}

// AsyncMux::split
std::vector<WriteBlock> AsyncMux::split(uint64_t offset, std::span<const Byte> data) {
    std::vector<WriteBlock> out;
    size_t cursor = 0;

    while (cursor < data.size()) {
        const size_t n = std::min<size_t>(kBlockSize, data.size() - cursor);
        std::vector<Byte> chunk(n);
        std::copy_n(data.begin() + cursor, n, chunk.begin());

        out.push_back(WriteBlock{
            .block_id = allocator_.next(),
            .file_offset = offset + cursor,
            .data = std::move(chunk),
        });
        cursor += n;
    }

    return out;
}

// AsyncMux::assemble
IoBuffer AsyncMux::assemble(const std::vector<BlockLocation>& locations,
                            std::vector<IoBuffer> pieces,
                            uint64_t read_offset,
                            uint64_t read_size) {
    IoBuffer out(static_cast<size_t>(read_size));

    for (size_t i = 0; i < locations.size(); ++i) {
        const auto& loc = locations[i];
        const auto& src = pieces[i].data;

        const size_t dst_offset = static_cast<size_t>(loc.file_offset - read_offset);
        const size_t n = static_cast<size_t>(loc.size);

        std::copy_n(src.begin(), n, out.data.begin() + dst_offset);
    }

    return out;
}

// FuseFrontend::FuseFrontend
FuseFrontend::FuseFrontend(AsyncMux& mux) : mux_(mux) {}

// FuseFrontend::on_read
cppcoro::task<IoBuffer> FuseFrontend::on_read(const std::string& path,
                                              uint64_t offset,
                                              uint64_t size) {
    co_return co_await mux_.read(path, offset, size);
}

// FuseFrontend::on_write
cppcoro::task<void> FuseFrontend::on_write(const std::string& path,
                                           uint64_t offset,
                                           std::span<const Byte> data) {
    co_await mux_.write(path, offset, data);
    co_return;
}

} // namespace asyncmux