#ifndef ASYNCMUX_METADATA_STORE_HH
#define ASYNCMUX_METADATA_STORE_HH

#include "asyncmux.hh"

#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace asyncmux {

class MetadataStore {
public:
    std::vector<BlockLocation> lookup(const std::string& path,
                                      uint64_t offset,
                                      uint64_t size) const;

    void update(const std::string& path,
                BlockId block_id,
                TierId tier_id,
                uint64_t file_offset,
                uint64_t size);

    void update_block(BlockId block_id, TierId new_tier);

    void relocate_block(BlockId block_id, TierId new_tier);

    LocatedBlock get_block(BlockId block_id) const;

    TierId tier_of(BlockId block_id) const;

    uint64_t version_of(const std::string& path) const;
    uint64_t bump_version(const std::string& path);

private:
    struct FileEntry {
        std::vector<BlockLocation> extents;
        uint64_t version = 0;
    };

    struct BlockIndexEntry {
        std::string path;
        uint64_t file_offset = 0;
        uint64_t size = 0;
    };

    void rebuild_block_index_locked(const std::string& path);

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, FileEntry> file_map_;
    std::unordered_map<BlockId, BlockIndexEntry> block_index_;
};

} // namespace asyncmux

#endif