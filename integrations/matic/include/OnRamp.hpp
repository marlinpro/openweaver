#ifndef MARLIN_ONRAMP_ETH_ONRAMP_HPP
#define MARLIN_ONRAMP_ETH_ONRAMP_HPP

#include <marlin/multicast/DefaultMulticastClient.hpp>
#include <marlin/pubsub/attestation/SigAttester.hpp>
#include <marlin/rlpx/RlpxTransportFactory.hpp>
#include <cryptopp/blake2.h>

using namespace marlin;
using namespace marlin::core;
using namespace marlin::asyncio;
using namespace marlin::beacon;
using namespace marlin::pubsub;
using namespace marlin::rlpx;
using namespace marlin::multicast;

class OnRamp {
public:
	DefaultMulticastClient<OnRamp, SigAttester, LpfBloomWitnesser> multicastClient;
	RlpxTransport<OnRamp> *rlpxt = nullptr;
	RlpxTransportFactory<OnRamp, OnRamp> f;

	std::vector<std::tuple<uint32_t, uint16_t, uint16_t>> get_protocols() {
		return {};
	}
	template<typename... Args>
	OnRamp(DefaultMulticastClientOptions clop, Args&&... args) : multicastClient(clop, std::forward<Args>(args)...), header(0) {
		multicastClient.delegate = this;
		f.bind(SocketAddress::loopback_ipv4(12121));
		f.listen(*this);
	}

	template<typename T> // TODO: Code smell, remove later
	void did_recv(
		DefaultMulticastClient<OnRamp, SigAttester, LpfBloomWitnesser> &,
		Buffer &&message,
		T,
		uint16_t channel,
		uint64_t message_id
	) {
		if(channel == 0 && (message_id & 0x0) == 0) {
			SPDLOG_INFO(
				"Received message {} on channel {}",
				message_id,
				channel
			);
		} else if(channel == 1 && (message_id & 0xf) == 0) {
			SPDLOG_INFO(
				"Received message {} on channel {}",
				message_id,
				channel
			);
		}

		if (rlpxt != nullptr) {
			SPDLOG_DEBUG(
				"Sending to blockchain client"
			);
			rlpxt->send(std::move(message));
		}
	}

	void did_subscribe(
		DefaultMulticastClient<OnRamp, SigAttester, LpfBloomWitnesser> &,
		uint16_t
	) {}

	void did_unsubscribe(
		DefaultMulticastClient<OnRamp, SigAttester, LpfBloomWitnesser> &,
		uint16_t
	) {}

	Buffer header;

	void did_recv(RlpxTransport<OnRamp> &transport, Buffer &&message) {
		CryptoPP::BLAKE2b blake2b((uint)8);
		blake2b.Update((uint8_t *)message.data(), message.size());
		uint64_t message_id;
		blake2b.TruncatedFinal((uint8_t *)&message_id, 8);
		SPDLOG_DEBUG(
			"Transport {{ Src: {}, Dst: {} }}: Did recv message {}: {} bytes",
			transport.src_addr.to_string(),
			transport.dst_addr.to_string(),
			message_id,
			message.size()
		);

		if(message.data()[0] == 0x10) { // eth63 Status
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: Status message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);

			Buffer response(message.size());

			std::memcpy(response.data(), message.data(), message.size());
			// response.data()[2] = 74;
			// std::memcpy(response.data()+7, "\x83\x02\x00\x00", 4);
			// std::memcpy(response.data() + 11, message.data() + message.size() - 33, 33);
			// std::memcpy(response.data() + 44, message.data() + message.size() - 33, 33);

			transport.send(std::move(response));
		} else if(message.data()[0] == 0x11) { // eth63 NewBlockHashes
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: NewBlockHashes message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);

			if(message.size() > 60) {
				// FIXME: Currently, doesn't work if more than one hash in message
				SPDLOG_ERROR(
					"Transport {{ Src: {}, Dst: {} }}: Too many hashes: {} bytes",
					transport.src_addr.to_string(),
					transport.dst_addr.to_string(),
					message.size()
				);

				return;
			}

			// Get header
			Buffer headermsg(38);
			headermsg.write_uint8_unsafe(0, 0x13);
			headermsg.write_uint8_unsafe(1, 0xe4);
			headermsg.write_unsafe(2, message.data() + 3, 33);
			headermsg.write_uint8_unsafe(35, 1);
			headermsg.write_uint8_unsafe(36, 0x80);
			headermsg.write_uint8_unsafe(37, 0x80);

			transport.send(std::move(headermsg));

			// Get body
			Buffer bodymsg(35);
			bodymsg.write_uint8_unsafe(0, 0x15);
			bodymsg.write_uint8_unsafe(1, 0xe1);
			bodymsg.write_unsafe(2, message.data() + 3, 33);

			transport.send(std::move(bodymsg));
		} else if(message.data()[0] == 0x13) { // eth63 GetBlockHeaders
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: GetBlockHeaders message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			transport.send(Buffer({0x14, 0xc0}, 2));
		} else if(message.data()[0] == 0x14) { // eth63 BlockHeaders
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: BlockHeaders message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			header = std::move(message);
		} else if(message.data()[0] == 0x15) { // eth63 GetBlockBodies
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: GetBlockBodies message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			transport.send(Buffer({0x16, 0xc0}, 2));
		} else if(message.data()[0] == 0x16) { // eth63 BlockBodies
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: BlockBodies message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);

			auto&& header = std::move(this->header);
			header.cover_unsafe(1);
			if(header.size() <= 56) {
				header.cover_unsafe(1);
			} else {
				uint8_t hll = header.read_uint8_unsafe(0) - 0xf7;
				header.cover_unsafe(1 + hll);
			}

			message.cover_unsafe(1);
			if(message.size() <= 56) {
				message.cover_unsafe(1);
			} else {
				uint8_t hll = message.read_uint8_unsafe(0) - 0xf7;
				message.cover_unsafe(1 + hll);
			}
			if(message.size() <= 56) {
				message.cover_unsafe(1);
			} else {
				uint8_t hll = message.read_uint8_unsafe(0) - 0xf7;
				message.cover_unsafe(1 + hll);
			}

			Buffer final(1 + 9 + 9 + header.size() + message.size() + 1);
			uint64_t pos = final.size();
			final.write_uint8_unsafe(pos - 1, 0x80); pos--;
			final.write_unsafe(pos - message.size(), message.data(), message.size()); pos -= message.size();
			final.write_unsafe(pos - header.size(), header.data(), header.size()); pos -= header.size();

			{
				uint64_t size = header.size() + message.size();
				uint8_t length = 0;
				while(size > 0) {
					final.write_uint8_unsafe(pos - 1, (uint8_t)size); pos--;
					length++;
					size = size >> 8;
				}
				final.write_uint8_unsafe(pos - 1, length + 0xf7); pos--;
			}

			{
				uint64_t size = final.size() - pos;
				uint8_t length = 0;
				while(size > 0) {
					final.write_uint8_unsafe(pos - 1, (uint8_t)size); pos--;
					length++;
					size = size >> 8;
				}
				final.write_uint8_unsafe(pos - 1, length + 0xf7); pos--;
			}

			final.write_uint8_unsafe(pos - 1, 0x17); pos--;
			final.cover_unsafe(pos);

			// SPDLOG_INFO(
			// 	"Transport {{ Src: {}, Dst: {} }}: Final message: {} bytes: {}",
			// 	transport.src_addr.to_string(),
			// 	transport.dst_addr.to_string(),
			// 	final.size(),
			// 	spdlog::to_hex(final.data(), final.data() + final.size())
			// );

			CryptoPP::BLAKE2b blake2b((uint)8);
			blake2b.Update((uint8_t *)final.data(), final.size());
			uint64_t message_id;
			blake2b.TruncatedFinal((uint8_t *)&message_id, 8);

			SPDLOG_INFO(
				"Received message {} on channel 0",
				message_id
			);
			multicastClient.ps.send_message_on_channel(
				0,
				message_id,
				final.data(),
				final.size()
			);

			// transport.send(std::move(final));
		} else if(message.data()[0] == 0x1d) { // eth63 GetNodeData
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: GetNodeData message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			transport.send(Buffer({0x1e, 0xc0}, 2));
		} else if(message.data()[0] == 0x12) { // eth63 Transactions
			SPDLOG_DEBUG(
				"Transport {{ Src: {}, Dst: {} }}: Transactions message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			// multicastClient.ps.send_message_on_channel(
			// 	1,
			// 	message_id,
			// 	message.data(),
			// 	message.size()
			// );
		} else if(message.data()[0] == 0x17) { // eth63 NewBlock
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: NewBlock message: {} bytes",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size()
			);
			SPDLOG_INFO(
				"Received message {} on channel 0",
				message_id
			);
			multicastClient.ps.send_message_on_channel(
				0,
				message_id,
				message.data(),
				message.size()
			);
		} else if(message.data()[0] == 0x18) { // eth65 PooledTransactions
			// Nothing
		} else {
			SPDLOG_INFO(
				"Transport {{ Src: {}, Dst: {} }}: Unknown message: {} bytes: {}",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				message.size(),
				spdlog::to_hex(message.data(), message.data() + message.size())
			);
		}
	}

	void did_send(
		RlpxTransport<OnRamp> &transport __attribute__((unused)),
		Buffer &&message __attribute__((unused))
	) {
		SPDLOG_DEBUG(
			"Transport {{ Src: {}, Dst: {} }}: Did send message: {} bytes",
			transport.src_addr.to_string(),
			transport.dst_addr.to_string(),
			message.size()
		);
	}

	void did_dial(RlpxTransport<OnRamp> &) {}

	bool should_accept(SocketAddress const &) {
		return true;
	}

	void did_create_transport(RlpxTransport<OnRamp> &transport) {
		rlpxt  = &transport;
		transport.setup(this);
	}

	void did_close(RlpxTransport<OnRamp> &, uint16_t) {
		rlpxt = nullptr;
	}
};

#endif // MARLIN_ONRAMP_ETH_ONRAMP_HPP
