#include "metadata_store.hh"

#include <algorithm>
#include <mutex>

namespace asyncmux {

std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
                                                 uint64_t offset,
                                                 uint64_t size) const {
    std::shared_lock lock(mu_);
    auto it = file_map_.find(path);
    if (it == file_map_.end() || size == 0) {
        return {};
    }

    std::vector<BlockLocation> out;
    const uint64_t end = offset + size;
    for (const auto& loc : it->second.extents) {
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
    const uint64_t new_end = file_offset + size;

    std::vector<BlockLocation> next;
    next.reserve(file.extents.size() + 2);

    for (const auto& loc : file.extents) {
        const uint64_t old_begin = loc.file_offset;
        const uint64_t old_end = loc.file_offset + loc.size;

        if (old_end <= new_begin || old_begin >= new_end) {
            next.push_back(loc);
            continue;
        }

        if (old_begin < new_begin) {
            next.push_back(BlockLocation{
                loc.block_id,
                loc.tier_id,
                old_begin,
                new_begin - old_begin,
            });
        }

        if (old_end > new_end) {
            next.push_back(BlockLocation{
                loc.block_id,
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

void MetadataStore::update_block(BlockId block_id, TierId new_tier) {
    relocate_block(block_id, new_tier);
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

} // namespace asyncmux