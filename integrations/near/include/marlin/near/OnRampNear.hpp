#ifndef MARLIN_ONRAMP_NEAR_HPP
#define MARLIN_ONRAMP_NEAR_HPP

#include <marlin/multicast/DefaultMulticastClient.hpp>
#include <marlin/near/NearTransportFactory.hpp>
#include <marlin/near/NearTransport.hpp>
#include <cryptopp/blake2.h>
#include <libbase58.h>
#include <boost/filesystem.hpp>

using namespace marlin::near;
using namespace marlin::core;
using namespace marlin::multicast;

namespace marlin {
namespace near {


class OnRampNear {
public:
	DefaultMulticastClient<OnRampNear> multicastClient;
	uint8_t static_sk[crypto_sign_SECRETKEYBYTES], static_pk[crypto_sign_PUBLICKEYBYTES]; // should be moved to DefaultMulticastClient and add a function there to sign a message.
	NearTransport<OnRampNear> *nearTransport = nullptr;
	NearTransportFactory<OnRampNear, OnRampNear> f;

	void handle_handshake(core::Buffer &&message);
	void handle_transaction(core::Buffer &&message);
	void handle_block(core::Buffer &&message);

	OnRampNear(DefaultMulticastClientOptions clop): multicastClient(clop) {
		multicastClient.delegate = this;

		if(sodium_init() == -1) {
			throw;
		}
		if(boost::filesystem::exists("./.marlin/keys/near_gateway")) {
			std::ifstream sk("./.marlin/keys/near_gateway", std::ios::binary);
			if(!sk.read((char *)static_sk, crypto_sign_SECRETKEYBYTES)) {
				throw;
			}
			crypto_sign_ed25519_sk_to_pk(static_pk, static_sk);
		} else {
			crypto_sign_keypair(static_pk, static_sk);

			boost::filesystem::create_directories("./.marlin/keys/");
			std::ofstream sk("./.marlin/keys/near_gateway", std::ios::binary);

			sk.write((char *)static_sk, crypto_sign_SECRETKEYBYTES);
		}

		char b58[65];
		size_t sz = 65;
		SPDLOG_DEBUG(
			"PrivateKey: {} \n PublicKey: {}",
			spdlog::to_hex(static_sk, static_sk + crypto_sign_SECRETKEYBYTES),
			spdlog::to_hex(static_pk, static_pk + crypto_sign_PUBLICKEYBYTES)
		);

		if(b58enc(b58, &sz, static_pk, 32)) {
			SPDLOG_INFO(
				"Node identity: {}",
				b58
			);
		} else {
			SPDLOG_ERROR("Failed to create base 58 of public key.");
		}

		f.bind(SocketAddress::loopback_ipv4(8000));
		f.listen(*this);
	}

	void did_recv(NearTransport <OnRampNear> &, Buffer &&message) {
		SPDLOG_DEBUG(
			"Message received from Near: {} bytes: {}",
			message.size(),
			spdlog::to_hex(message.data(), message.data() + message.size())
		);
		SPDLOG_INFO(
			"Message received from Near: {} bytes",
			message.size()
		);
		if(message.data()[0] == 0x10) {
			handle_handshake(std::move(message));
		} else if(message.data()[0] == 0xc) {
			handle_transaction(std::move(message));
		} else if(message.data()[0] == 0xb) {
			handle_block(std::move(message));
		} else if(message.data()[0] == 0xd) {
			SPDLOG_DEBUG(
				"This is a RoutedMessage"
			);
		} else {
			SPDLOG_WARN(
				"Unhandled: {}",
				message.data()[0]
			);
		}
	}

	template<typename T> // TODO: Code smell, remove later
	void did_recv(
		DefaultMulticastClient<OnRampNear> &,
		Buffer &&bytes,
		T,
		uint16_t,
		uint64_t
	) {
		SPDLOG_DEBUG(
			"OnRampNear:: did_recv, forwarding message: {}",
			spdlog::to_hex(bytes.data(), bytes.size())
		);
		if(nearTransport != nullptr) {
			nearTransport->send(std::move(bytes));
		}
	}

	void did_send_message(NearTransport<OnRampNear> &, Buffer &&message [[maybe_unused]]) {
		SPDLOG_DEBUG(
			"Transport: Did send message: {} bytes",
			// transport.src_addr.to_string(),
			// transport.dst_addr.to_string(),
			message.size()
		);
	}

	void did_dial(NearTransport<OnRampNear> &) {
	}

	bool should_accept(SocketAddress const &) {
		return true;
	}


	void did_subscribe(
		DefaultMulticastClient<OnRampNear> &,
		uint16_t
	) {}

	void did_unsubscribe(
		DefaultMulticastClient<OnRampNear> &,
		uint16_t
	) {}

	void did_close(NearTransport<OnRampNear> &, uint16_t) {
		nearTransport = nullptr;
	}

	void did_create_transport(NearTransport <OnRampNear> &transport) {
		nearTransport = &transport;
		transport.setup(this);
	}
};


void OnRampNear::handle_transaction(core::Buffer &&message) {
	SPDLOG_DEBUG(
		"Handling transaction: {}",
		spdlog::to_hex(message.data(), message.data() + message.size())
	);
	message.uncover_unsafe(4);
	CryptoPP::BLAKE2b blake2b((uint)8);
	blake2b.Update((uint8_t *)message.data(), message.size());
	uint64_t message_id;
	blake2b.TruncatedFinal((uint8_t *)&message_id, 8);
	multicastClient.ps.send_message_on_channel(
		1,
		message_id,
		message.data(),
		message.size()
	);
}

void OnRampNear::handle_block(core::Buffer &&message) {
	SPDLOG_DEBUG("Handling block");
	message.uncover_unsafe(4);
	CryptoPP::BLAKE2b blake2b((uint)8);
	blake2b.Update((uint8_t *)message.data(), message.size());
	uint64_t message_id;
	blake2b.TruncatedFinal((uint8_t *)&message_id, 8);
	multicastClient.ps.send_message_on_channel(
		0,
		message_id,
		message.data(),
		message.size()
	);
}

void OnRampNear::handle_handshake(core::Buffer &&message) {
	SPDLOG_DEBUG("Replying");
	uint8_t *buf = message.data();
	uint32_t buf_size = message.size();
	std::swap_ranges(buf + 9, buf + 42, buf + 42);

	uint8_t near_key_offset = 42, gateway_key_offset = 9;

	using namespace CryptoPP;
	CryptoPP::SHA256 sha256;
	uint8_t hashed_message[32];

	int flag = std::memcmp(buf + near_key_offset, buf + gateway_key_offset, 33);
	if(flag < 0) {
		sha256.Update(buf + near_key_offset, 33);
		sha256.Update(buf + gateway_key_offset, 33);
	} else {
		sha256.Update(buf + gateway_key_offset, 33);
		sha256.Update(buf + near_key_offset, 33);
	}
	sha256.Update(buf + buf_size - 73, 8);
	sha256.TruncatedFinal(hashed_message, 32);
	uint8_t *near_node_signature = buf + buf_size - crypto_sign_BYTES;

	if(crypto_sign_verify_detached(near_node_signature, hashed_message, 32, buf + near_key_offset + 1) != 0) {
		SPDLOG_ERROR("Signature verification failed");
		return;
	} else {
		SPDLOG_DEBUG("Signature verified successfully");
	}

	uint8_t *mySignature = buf + buf_size - 64;
	crypto_sign_detached(mySignature, NULL, hashed_message, 32, static_sk);
	message.uncover_unsafe(4);
	nearTransport->send(std::move(message));
}


} // near
} // marlin

#endif // MARLIN_ONRAMP_NEAR_HPP
