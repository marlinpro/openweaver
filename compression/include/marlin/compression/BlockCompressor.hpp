#ifndef MARLIN_COMPRESSION_BLOCKCOMPRESSOR_HPP
#define MARLIN_COMPRESSION_BLOCKCOMPRESSOR_HPP

#include <marlin/core/Buffer.hpp>
#include <cryptopp/blake2.h>

#include <unordered_map>
#include <numeric>


namespace marlin {
namespace compression {

struct BlockCompressor {
private:
	std::unordered_map<uint64_t, core::Buffer> txns;
	std::unordered_map<uint64_t, uint64_t> txn_seen;
public:
	void add_txn(core::Buffer&& txn, uint64_t timestamp) {
		CryptoPP::BLAKE2b blake2b((uint)8);
		blake2b.Update(txn.data(), txn.size());
		uint64_t txn_id;
		blake2b.TruncatedFinal((uint8_t*)&txn_id, 8);

		txns.try_emplace(
			txn_id,
			std::move(txn)
		);

		txn_seen[txn_id] = timestamp;
	}

	void remove_txn(core::Buffer&& txn, uint64_t timestamp) {
		CryptoPP::BLAKE2b blake2b((uint)8);
		blake2b.Update(txn.data(), txn.size());
		uint64_t txn_id;
		blake2b.TruncatedFinal((uint8_t*)&txn_id, 8);

		txns.erase(txn_id);
		txn_seen.erase(txn_id);
	}

	core::Buffer compress(
		std::vector<core::WeakBuffer> const& misc_bufs,
		std::vector<core::WeakBuffer> const& txn_bufs
	) {
		// Compute total size of misc bufs
		size_t misc_size = std::transform_reduce(
			misc_bufs.begin(),
			misc_bufs.end(),
			0,
			std::plus<>(),
			[](core::WeakBuffer const& buf) { return buf.size(); }
		);
		size_t total_size = misc_size + txn_bufs.size()*8 + 10000; // Add space for txn ids and extra space for any full size txns
		core::Buffer final_buf(total_size);
		size_t offset = 0;

		// Copy misc bufs
		final_buf.write_uint64_le_unsafe(offset, misc_size + 8*misc_bufs.size());
		offset += 8;
		for(auto iter = misc_bufs.begin(); iter != misc_bufs.end(); iter++) {
			final_buf.write_uint64_le_unsafe(offset, iter->size());
			offset += 8;
			final_buf.write_unsafe(offset, iter->data(), iter->size());
			offset += iter->size();
		}

		CryptoPP::BLAKE2b blake2b((uint)8);
		for(auto iter = txn_bufs.begin(); iter != txn_bufs.end(); iter++) {
			blake2b.Update(iter->data(), iter->size());
			uint64_t txn_id;
			blake2b.TruncatedFinal((uint8_t*)&txn_id, 8);
			auto txn_iter = txns.find(txn_id);

			if(txn_iter == txns.end()) {
				// Txn not in cache, encode in full
				if(offset + txn_bufs.size()*8 > total_size)
					throw; // TODO: Handle insufficient buffer size

				final_buf.write_uint8_unsafe(offset, 0x00);
				final_buf.write_uint64_le_unsafe(offset+1, iter->size());
				final_buf.write_unsafe(offset+9, iter->data(), iter->size());

				offset += 9 + iter->size();
			} else {
				// Found txn in cache, copy id
				final_buf.write_uint8_unsafe(offset, 0x01);
				final_buf.write_uint64_unsafe(offset+1, txn_id);
				// Note: Write txn_id without endian conversions,
				// the hash was directly copied to txn_id memory

				offset += 9;
			}
		}

		final_buf.truncate_unsafe(final_buf.size() - offset);

		return final_buf;
	}

	std::optional<std::tuple<std::vector<core::Buffer>, std::vector<core::Buffer>>> decompress(core::WeakBuffer const& buf) {
		std::vector<core::Buffer> misc_bufs, txn_bufs;

		// Bounds check
		if(buf.size() < 8) return std::nullopt;
		// Read misc size
		auto misc_size = buf.read_uint64_le_unsafe(0);
		// Bounds check
		if(buf.size() < 8 + misc_size) return std::nullopt;

		size_t offset = 8;
		// Read misc items
		while(offset < 8 + misc_size) {
			// Bounds check
			if(buf.size() < offset + 8) return std::nullopt;
			// Read misc item size
			auto item_size = buf.read_uint64_le_unsafe(offset);
			// Bounds check
			if(buf.size() < offset + 8 + item_size) return std::nullopt;
			// Copy misc item
			core::Buffer item(item_size);
			buf.read_unsafe(offset + 8, item.data(), item_size);
			misc_bufs.push_back(std::move(item));

			offset += 8 + item_size;
		}

		// Malformed block
		if(offset != 8 + misc_size) return std::nullopt;

		// Read txns
		while(offset < buf.size()) {
			// Bounds check
			if(buf.size() < offset + 9) return std::nullopt;

			// Check type of txn encoding
			uint8_t type = buf.read_uint8_unsafe(offset);
			if(type == 0x00) { // Full txn
				// Get txn size
				auto txn_size = buf.read_uint64_le_unsafe(offset + 1);
				// Bounds check
				if(buf.size() < offset + 9 + txn_size) return std::nullopt;

				// Copy txn
				core::Buffer txn(txn_size);
				buf.read_unsafe(offset + 9, txn.data(), txn_size);
				txn_bufs.push_back(std::move(txn));

				offset += 9 + txn_size;
			} else if(type == 0x01) { // Txn id
				// Get txn id
				auto txn_id = buf.read_uint64_unsafe(offset + 1);
				// Note: Read txn_id without endian conversions,
				// the hash was directly copied to txn_id memory

				// Find txn
				auto iter = txns.find(txn_id);
				if(iter == txns.end()) return std::nullopt;

				// Copy txn
				core::Buffer txn(iter->second.size());
				txn.write_unsafe(0, iter->second.data(), iter->second.size());
				txn_bufs.push_back(std::move(txn));

				offset += 9;
			} else {
				return std::nullopt;
			}
		}

		return std::make_tuple(std::move(misc_bufs), std::move(txn_bufs));
	}
};

} // namespace compression
} // namespace marlin

#endif // MARLIN_COMPRESSION_BLOCKCOMPRESSOR_HPP
