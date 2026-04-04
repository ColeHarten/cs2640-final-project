#include "asyncmux.hh"

namespace asyncmux {

IoBuffer::IoBuffer() = default;

IoBuffer::IoBuffer(std::size_t n) : data(n) {}

IoBuffer::IoBuffer(std::vector<Byte> bytes) : data(std::move(bytes)) {}

std::size_t IoBuffer::size() const {
	return data.size();
}

Byte* IoBuffer::bytes() {
	return data.data();
}

const Byte* IoBuffer::bytes() const {
	return data.data();
}

std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
												 std::uint64_t offset,
												 std::uint64_t size) const {
	std::shared_lock lock(mu_);
	auto it = file_map_.find(path);
	if (it == file_map_.end()) {
		return {};
	}

	std::vector<BlockLocation> out;
	const auto end = offset + size;
	for (const auto& loc : it->second) {
		const auto loc_begin = loc.file_offset;
		const auto loc_end = loc.file_offset + loc.size;
		if (loc_end <= offset || loc_begin >= end) {
			continue;
		}
		out.push_back(loc);
	}
	return out;
}

void MetadataStore::update(const std::string& path,
						   BlockId block_id,
						   TierId tier_id,
						   std::uint64_t file_offset,
						   std::uint64_t size) {
	std::unique_lock lock(mu_);
	auto& vec = file_map_[path];
	auto it = std::find_if(vec.begin(), vec.end(), [&](const BlockLocation& loc) {
		return loc.block_id == block_id;
	});

	if (it == vec.end()) {
		vec.push_back(BlockLocation{block_id, tier_id, file_offset, 0, size});
	} else {
		it->tier_id = tier_id;
		it->file_offset = file_offset;
		it->block_offset = 0;
		it->size = size;
	}
}

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
}

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

MemoryTier::MemoryTier(TierId id, std::string name, cppcoro::static_thread_pool& pool)
	: id_(id), name_(std::move(name)), pool_(pool) {}

TierId MemoryTier::id() const {
	return id_;
}

std::string MemoryTier::name() const {
	return name_;
}

cppcoro::task<IoBuffer> MemoryTier::read_block(BlockId block_id,
											   std::uint64_t offset,
											   std::uint64_t size) {
	co_await pool_.schedule();

	std::lock_guard lock(mu_);
	auto it = blocks_.find(block_id);
	if (it == blocks_.end()) {
		throw IoError("read_block: missing block");
	}
	if (offset > it->second.size()) {
		throw IoError("read_block: offset out of range");
	}

	const auto available = it->second.size() - static_cast<std::size_t>(offset);
	const auto n = std::min<std::size_t>(size, available);
	std::vector<Byte> out(n);
	std::copy_n(it->second.data() + offset, n, out.data());
	co_return IoBuffer{std::move(out)};
}

cppcoro::task<void> MemoryTier::write_block(BlockId block_id,
											std::uint64_t offset,
											std::span<const Byte> data) {
	co_await pool_.schedule();

	std::lock_guard lock(mu_);
	auto& dst = blocks_[block_id];
	const std::size_t needed = static_cast<std::size_t>(offset) + data.size();
	if (dst.size() < needed) {
		dst.resize(needed);
	}
	std::copy(data.begin(), data.end(), dst.begin() + offset);
	co_return;
}

cppcoro::task<void> MemoryTier::delete_block(BlockId block_id) {
	co_await pool_.schedule();

	std::lock_guard lock(mu_);
	blocks_.erase(block_id);
	co_return;
}

void TierRegistry::add(std::unique_ptr<Tier> tier) {
	tiers_.emplace(tier->id(), std::move(tier));
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

SimplePlacementPolicy::SimplePlacementPolicy(TierId default_tier) : default_tier_(default_tier) {}

TierId SimplePlacementPolicy::choose_tier(const WriteBlock&) {
	return default_tier_;
}

BlockId BlockAllocator::next() {
	return next_id_++;
}

AsyncMux::AsyncMux(TierRegistry& tiers,
				   MetadataStore& metadata,
				   PlacementPolicy& placement,
				   cppcoro::static_thread_pool& pool)
	: tiers_(tiers), metadata_(metadata), placement_(placement), pool_(pool) {}

cppcoro::task<IoBuffer> AsyncMux::read(const std::string& path,
									   std::uint64_t offset,
									   std::uint64_t size) {
	auto blocks = metadata_.lookup(path, offset, size);
	std::vector<cppcoro::task<IoBuffer>> tasks;
	tasks.reserve(blocks.size());

	for (const auto& b : blocks) {
		Tier& tier = tiers_.get(b.tier_id);
		tasks.emplace_back(tier.read_block(b.block_id, b.block_offset, b.size));
	}

	auto results = co_await cppcoro::when_all(std::move(tasks));
	co_return assemble(std::move(blocks), std::move(results), offset, size);
}

cppcoro::task<void> AsyncMux::write(const std::string& path,
									std::uint64_t offset,
									std::span<const Byte> data) {
	auto blocks = split(offset, data);

	for (auto& block : blocks) {
		TierId tier_id = placement_.choose_tier(block);
		Tier& tier = tiers_.get(tier_id);

		co_await tier.write_block(
			block.block_id,
			0,
			std::span<const Byte>(block.data.data(), block.data.size()));
		metadata_.update(path, block.block_id, tier_id, block.file_offset, block.data.size());
	}
}

cppcoro::task<void> AsyncMux::migrate(BlockId block_id, TierId src_id, TierId dst_id) {
	Tier& src = tiers_.get(src_id);
	Tier& dst = tiers_.get(dst_id);

	auto data = co_await src.read_block(block_id, 0, kBlockSize);
	co_await dst.write_block(block_id,
							 0,
							 std::span<const Byte>(data.data.data(), data.data.size()));
	metadata_.update_block(block_id, dst_id);
	co_await src.delete_block(block_id);
}

cppcoro::task<void> AsyncMux::promote(BlockId block_id, TierId hot_tier) {
	const TierId current = metadata_.tier_of(block_id);
	if (current == hot_tier) {
		co_return;
	}
	co_await migrate(block_id, current, hot_tier);
}

std::vector<WriteBlock> AsyncMux::split(std::uint64_t offset, std::span<const Byte> data) {
	std::vector<WriteBlock> out;
	std::size_t cursor = 0;

	while (cursor < data.size()) {
		const std::size_t n = std::min<std::size_t>(kBlockSize, data.size() - cursor);
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

IoBuffer AsyncMux::assemble(const std::vector<BlockLocation>& locations,
							std::vector<IoBuffer> blocks,
							std::uint64_t read_offset,
							std::uint64_t read_size) {
	IoBuffer out(static_cast<std::size_t>(read_size));

	for (std::size_t i = 0; i < locations.size(); ++i) {
		const auto& loc = locations[i];
		const auto& src = blocks[i].data;

		const std::uint64_t copy_begin = std::max<std::uint64_t>(loc.file_offset, read_offset);
		const std::uint64_t copy_end = std::min<std::uint64_t>(loc.file_offset + loc.size,
															   read_offset + read_size);
		if (copy_begin >= copy_end) {
			continue;
		}

		const std::size_t src_offset = static_cast<std::size_t>(copy_begin - loc.file_offset);
		const std::size_t dst_offset = static_cast<std::size_t>(copy_begin - read_offset);
		const std::size_t n = static_cast<std::size_t>(copy_end - copy_begin);

		std::copy_n(src.begin() + src_offset, n, out.data.begin() + dst_offset);
	}

	return out;
}

FuseFrontend::FuseFrontend(AsyncMux& mux) : mux_(mux) {}

cppcoro::task<IoBuffer> FuseFrontend::on_read(const std::string& path,
											  std::uint64_t offset,
											  std::uint64_t size) {
	co_return co_await mux_.read(path, offset, size);
}

cppcoro::task<void> FuseFrontend::on_write(const std::string& path,
										   std::uint64_t offset,
										   std::span<const Byte> data) {
	co_await mux_.write(path, offset, data);
}

} // namespace asyncmux
