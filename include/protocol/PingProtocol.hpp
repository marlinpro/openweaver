#ifndef MARLIN_BEACON_PINGPROTOCOL_HPP
#define MARLIN_BEACON_PINGPROTOCOL_HPP

#include <marlin/net/Buffer.hpp>
#include <marlin/net/SocketAddress.hpp>

#include <spdlog/spdlog.h>

namespace marlin {
namespace beacon {

struct PingStorage {};

template<typename NodeType>
class PingProtocol {
public:
	static void did_receive_packet(
		NodeType &node,
		const net::Buffer &&p,
		const net::SocketAddress &addr
	);

	static void did_send_packet(
		NodeType &node,
		const net::Buffer &&p,
		const net::SocketAddress &addr
	);

	static void did_receive_PING(NodeType &node, const net::SocketAddress &addr);
	static void send_PING(NodeType &node, const net::SocketAddress &addr);

	static void did_receive_PONG(NodeType &node, const net::SocketAddress &addr);
	static void send_PONG(NodeType &node, const net::SocketAddress &addr);
};


// Impl

struct PingPacket: public net::Buffer {
	uint8_t version() const {
		return read_uint8(0);
	}

	uint8_t message() const {
		return read_uint8(1);
	}
};

template<typename NodeType>
void PingProtocol<NodeType>::did_receive_packet(
	NodeType &node,
	const net::Buffer &&p,
	const net::SocketAddress &addr
) {
	auto pp = reinterpret_cast<const PingPacket *>(&p);
	switch(pp->message()) {
		// PING
		case 0: did_receive_PING(node, addr);
		break;
		// PONG
		case 1: did_receive_PONG(node, addr);
		break;
		// UNKNOWN
		default: SPDLOG_DEBUG("UNKNOWN <<< {}", addr.to_string());
		break;
	}
}

template<typename NodeType>
void PingProtocol<NodeType>::did_send_packet(
	NodeType &,
	const net::Buffer &&p,
	const net::SocketAddress &addr __attribute__((unused))
) {
	auto pp = reinterpret_cast<const PingPacket *>(&p);
	switch(pp->message()) {
		// PING
		case 0: SPDLOG_DEBUG("PING >>> {}", addr.to_string());
		break;
		// PONG
		case 1: SPDLOG_DEBUG("PONG >>> {}", addr.to_string());
		break;
		// UNKNOWN
		default: SPDLOG_DEBUG("UNKNOWN >>> {}", addr.to_string());
		break;
	}
}

template<typename NodeType>
void PingProtocol<NodeType>::send_PING(
	NodeType &node,
	const net::SocketAddress &addr
) {
	char *message = new char[10] {0, 0};

	net::Buffer p(message, 10);
	node.send(std::move(p), addr);
}

template<typename NodeType>
void PingProtocol<NodeType>::did_receive_PING(
	NodeType &node,
	const net::SocketAddress &addr
) {
	SPDLOG_DEBUG("PING <<< {}", addr.to_string());

	send_PONG(node, addr);
}

template<typename NodeType>
void PingProtocol<NodeType>::send_PONG(
	NodeType &node,
	const net::SocketAddress &addr
) {
	char *message = new char[10] {0, 1};

	net::Buffer p(message, 10);
	node.send(std::move(p), addr);
}

template<typename NodeType>
void PingProtocol<NodeType>::did_receive_PONG(
	NodeType &,
	const net::SocketAddress &addr __attribute__((unused))
) {
	SPDLOG_DEBUG("PONG <<< {}", addr.to_string());
}

} // namespace beacon
} // namespace marlin

#endif // MARLIN_BEACON_PINGPROTOCOL_HPP
