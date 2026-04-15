#include "asyncmux.hh"
#include "tier.hh"

#include <algorithm>
#include <iterator>

namespace asyncmux {

IoBuffer::IoBuffer() = default;
IoBuffer::IoBuffer(size_t n) : data(n) {}
IoBuffer::IoBuffer(std::vector<Byte> bytes) : data(std::move(bytes)) {}

size_t IoBuffer::size() const {
    return data.size();
}

Byte* IoBuffer::bytes() {
    return data.data();
}

const Byte* IoBuffer::bytes() const {
    return data.data();
}

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
        prepared.push_back(PreparedWrite{std::move(block), placement_.choose_tier(block)});
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
        const size_t n = std::min<size_t>(kBlockSize, data.size() - cursor);
        std::vector<Byte> chunk(n);
        std::copy_n(data.begin() + static_cast<std::ptrdiff_t>(cursor), n, chunk.begin());

        out.push_back(WriteBlock{
            .block_id = allocator_.next(),
            .file_offset = offset + cursor,
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
