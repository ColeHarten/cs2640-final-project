#include "asyncmux.hh"

namespace asyncmux {

// IoBuffer::IoBuffer
//         Default-constructs an empty byte buffer.
IoBuffer::IoBuffer() = default;

// IoBuffer::IoBuffer
//         Constructs a buffer pre-sized to n bytes.
IoBuffer::IoBuffer(size_t n) : data(n) {}

// IoBuffer::IoBuffer
//         Takes ownership of an existing byte vector.
IoBuffer::IoBuffer(std::vector<Byte> bytes) : data(std::move(bytes)) {}

// IoBuffer::size
//         Returns the number of bytes in the buffer.
size_t IoBuffer::size() const {
	return data.size();
}

// IoBuffer::bytes
//         Returns a mutable pointer to the buffer data.
Byte* IoBuffer::bytes() {
	return data.data();
}

// IoBuffer::bytes
//         Returns a const pointer to the buffer data.
const Byte* IoBuffer::bytes() const {
	return data.data();
}

// MetadataStore::lookup
//         Finds block locations overlapping the requested file range.
std::vector<BlockLocation> MetadataStore::lookup(const std::string& path,
												 uint64_t offset,
												 uint64_t size) const {
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

// MetadataStore::update
//         Inserts or refreshes metadata for a block at a file offset.
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
		vec.push_back(BlockLocation{block_id, tier_id, file_offset, 0, size});
	} else {
		it->tier_id = tier_id;
		it->file_offset = file_offset;
		it->block_offset = 0;
		it->size = size;
	}
}

// MetadataStore::update_block
//         Moves a known block's metadata to a new tier id.
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

// MetadataStore::tier_of
//         Returns the current tier id for a specific block.
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
//         Registers a tier instance keyed by its id.
void TierRegistry::add(std::unique_ptr<Tier> tier) {
	tiers_.emplace(tier->id(), std::move(tier));
}

// TierRegistry::get
//         Returns a mutable reference to a tier by id.
Tier& TierRegistry::get(TierId id) {
	auto it = tiers_.find(id);
	if (it == tiers_.end()) {
		throw IoError("unknown tier");
	}
	return *it->second;
}

// TierRegistry::get
//         Returns a const reference to a tier by id.
const Tier& TierRegistry::get(TierId id) const {
	auto it = tiers_.find(id);
	if (it == tiers_.end()) {
		throw IoError("unknown tier");
	}
	return *it->second;
}

// SimplePlacementPolicy::SimplePlacementPolicy
//         Sets the default tier used for new writes.
SimplePlacementPolicy::SimplePlacementPolicy(TierId default_tier) : default_tier_(default_tier) {}

// SimplePlacementPolicy::choose_tier
//         Chooses the destination tier for a write block.
TierId SimplePlacementPolicy::choose_tier(const WriteBlock&) {
	return default_tier_;
}

// BlockAllocator::next
//         Returns the next monotonically increasing block id.
BlockId BlockAllocator::next() {
	return next_id_++;
}

// AsyncMux::AsyncMux
//         Constructs the multiplexer with shared service dependencies.
AsyncMux::AsyncMux(TierRegistry& tiers,
				   MetadataStore& metadata,
				   PlacementPolicy& placement,
				   cppcoro::static_thread_pool& pool)
	: tiers_(tiers), metadata_(metadata), placement_(placement), pool_(pool) {}

// AsyncMux::read
//         Reads file data by gathering and assembling mapped blocks.
cppcoro::task<IoBuffer> AsyncMux::read(const std::string& path,
									   uint64_t offset,
									   uint64_t size) {
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

// AsyncMux::write
//         Splits input data into blocks and writes them to tiers.
cppcoro::task<void> AsyncMux::write(const std::string& path,
									uint64_t offset,
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

// AsyncMux::migrate
//         Copies a block from one tier to another and updates metadata.
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

// AsyncMux::promote
//         Moves a block to a hotter tier when needed.
cppcoro::task<void> AsyncMux::promote(BlockId block_id, TierId hot_tier) {
	const TierId current = metadata_.tier_of(block_id);
	if (current == hot_tier) {
		co_return;
	}
	co_await migrate(block_id, current, hot_tier);
}

// AsyncMux::split
//         Breaks a byte range into fixed-size write blocks.
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
//         Reconstructs a contiguous read buffer from block fragments.
IoBuffer AsyncMux::assemble(const std::vector<BlockLocation>& locations,
							std::vector<IoBuffer> blocks,
							uint64_t read_offset,
							uint64_t read_size) {
	IoBuffer out(static_cast<size_t>(read_size));

	for (size_t i = 0; i < locations.size(); ++i) {
		const auto& loc = locations[i];
		const auto& src = blocks[i].data;

		const uint64_t copy_begin = std::max<uint64_t>(loc.file_offset, read_offset);
		const uint64_t copy_end = std::min<uint64_t>(loc.file_offset + loc.size,
															   read_offset + read_size);
		if (copy_begin >= copy_end) {
			continue;
		}

		const size_t src_offset = static_cast<size_t>(copy_begin - loc.file_offset);
		const size_t dst_offset = static_cast<size_t>(copy_begin - read_offset);
		const size_t n = static_cast<size_t>(copy_end - copy_begin);

		std::copy_n(src.begin() + src_offset, n, out.data.begin() + dst_offset);
	}

	return out;
}

// FuseFrontend::FuseFrontend
//         Creates a frontend adapter around AsyncMux operations.
FuseFrontend::FuseFrontend(AsyncMux& mux) : mux_(mux) {}

// FuseFrontend::on_read
//         Handles a frontend read request through AsyncMux.
cppcoro::task<IoBuffer> FuseFrontend::on_read(const std::string& path,
										  uint64_t offset,
										  uint64_t size) {
	co_return co_await mux_.read(path, offset, size);
}

// FuseFrontend::on_write
//         Handles a frontend write request through AsyncMux.
cppcoro::task<void> FuseFrontend::on_write(const std::string& path,
										   uint64_t offset,
										   std::span<const Byte> data) {
	co_await mux_.write(path, offset, data);
}

} // namespace asyncmux
