#ifndef MARLIN_PUBSUB_PUBSUBNODE_HPP
#define MARLIN_PUBSUB_PUBSUBNODE_HPP

#include <marlin/net/Node.hpp>
#include <marlin/stream/protocol/StreamProtocol.hpp>

#include <map>
#include <string>
#include <list>
#include <iostream>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bin_to_hex.h>

namespace marlin {
namespace pubsub {

typedef std::list<net::SocketAddress> ListSocketAddress;
typedef std::map<std::string, ListSocketAddress > MapChannelToAddresses;

template<typename PubSubDelegate>
class PubSubNode : public net::Node<PubSubNode<PubSubDelegate>, stream::StreamProtocol> {
public:
	typedef net::Node<PubSubNode<PubSubDelegate>, stream::StreamProtocol> BaseNode;

	stream::StreamStorage<PubSubNode> stream_storage;

	PubSubNode(const net::SocketAddress &_addr);

	std::list<net::Packet> message_buffer;
	uint64_t message_length = 0;
	uint64_t bytes_remaining = 0;
	std::string channel;

	PubSubDelegate delegate;

	void did_receive_bytes(net::Packet &&p, uint16_t stream_id, const net::SocketAddress &addr);

	void did_receive_SUBSCRIBE(net::Packet &&p, uint16_t, const net::SocketAddress &);
	void send_SUBSCRIBE(const net::SocketAddress &addr, std::string channel);

	void did_receive_UNSUBSCRIBE(net::Packet &&p, uint16_t, const net::SocketAddress &);
	void send_UNSUBSCRIBE(const net::SocketAddress &addr, std::string channel);

	void did_receive_RESPONSE(net::Packet &&, uint16_t, const net::SocketAddress &);
	void send_RESPONSE(bool success, std::string msg_string, const net::SocketAddress &);

	void did_receive_MESSAGE(net::Packet &&p, uint16_t, const net::SocketAddress &);
	void send_MESSAGE(const net::SocketAddress &addr, std::string channel, const char *data, uint64_t size);

	void send_message_on_channel(std::string channel, const char *data, uint64_t size);

	private:
	MapChannelToAddresses channel_subscription_map;
};


// Impl

template<typename PubSubDelegate>
PubSubNode<PubSubDelegate>::PubSubNode(const net::SocketAddress &_addr) : BaseNode(_addr) {
	stream::StreamProtocol<PubSubNode>::setup(*this);
	spdlog::default_logger()->set_level(spdlog::level::info);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::did_receive_bytes(net::Packet &&p, uint16_t stream_id, const net::SocketAddress &addr) {
	// Abort on empty message
	if(p.size() == 0)
		return;

	// Check if it is part of previously incomplete message
	if(bytes_remaining > 0) {
		this-> did_receive_MESSAGE(std::move(p), stream_id, addr);
		return;
	}

	uint8_t message_type = p.data()[0];

	// Hide message type
	p.cover(1);

	switch(message_type) {
		// SUBSCRIBE
		case 0: this->did_receive_SUBSCRIBE(std::move(p), stream_id, addr);
		break;
		// UNSUBSCRIBE
		case 1: this->did_receive_UNSUBSCRIBE(std::move(p), stream_id, addr);
		break;
		// RESPONSE
		case 2: this->did_receive_RESPONSE(std::move(p), stream_id, addr);
		break;
		// MESSAGE
		case 3: this->did_receive_MESSAGE(std::move(p), stream_id, addr);
		break;
	}
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::did_receive_SUBSCRIBE(net::Packet &&p, uint16_t, const net::SocketAddress &addr) {
	std::string channel(p.data(), p.data()+p.size());

	channel_subscription_map[channel].push_back(addr);

	spdlog::info("Received subscribe on channel {} from {}", channel, addr.to_string());

	// Send response
	send_RESPONSE(true, "SUBCRIBED TO CHANNEL: " + channel, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::send_SUBSCRIBE(const net::SocketAddress &addr, std::string channel) {
	char *message = new char[channel.size()+1];

	message[0] = 0;
	std::memcpy(message + 1, channel.data(), channel.size());

	std::unique_ptr<char[]> p(message);

	spdlog::info("Sending subscribe on channel {}", channel);

	stream::StreamProtocol<PubSubNode>::send_data(*this, std::move(p), channel.size() + 1, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::did_receive_UNSUBSCRIBE(net::Packet &&p, uint16_t, const net::SocketAddress &addr) {
	std::string channel(p.data(), p.data()+p.size());

	channel_subscription_map[channel].remove(addr);

	spdlog::info("Received unsubscribe on channel {} from {}", channel, addr.to_string());

	// Send response
	send_RESPONSE(true, "UNSUBCRIBED FROM CHANNEL: " + channel, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::send_UNSUBSCRIBE(const net::SocketAddress &addr, std::string channel) {
	char *message = new char[channel.size()+1];

	message[0] = 1;
	std::memcpy(message + 1, channel.data(), channel.size());

	std::unique_ptr<char[]> p(message);

	spdlog::info("Sending unsubscribe on channel {}", channel);

	stream::StreamProtocol<PubSubNode>::send_data(*this, std::move(p), channel.size() + 1, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::did_receive_RESPONSE(net::Packet &&p, uint16_t, const net::SocketAddress &) {
	bool success = p.data()[0];

	// Hide success
	p.cover(1);

	// process rest of the message
	std::string message(p.data(), p.data()+p.size());
	spdlog::info("Received {} response: {}", success == 0 ? "ERROR" : "OK", spdlog::to_hex(message.data(), message.data()+message.size()));
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::send_RESPONSE(bool success, std::string msg_string, const net::SocketAddress &addr) {
	// 0 for ERROR
	// 1 for OK
	uint64_t tot_msg_size = msg_string.size()+2;
	char *message = new char[tot_msg_size];

	message[0] = 2;

	if (success) {
		message[1] = 1;
	}
	else {
		message[1] = 0;
	}
	std::memcpy(message + 2, msg_string.data(), msg_string.size());

	std::unique_ptr<char[]> p(message);

	spdlog::info("Sending {} response: {}", success == 0 ? "ERROR" : "OK", spdlog::to_hex(message, message + tot_msg_size));

	stream::StreamProtocol<PubSubNode>::send_data(*this, std::move(p), tot_msg_size, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::did_receive_MESSAGE(net::Packet &&p, uint16_t, const net::SocketAddress &) {
	if(bytes_remaining == 0) { // New message
		// Check overflow
		if(p.size() < 8)
			return;

		uint64_t n_length;
		std::memcpy(&n_length, p.data(), 8);
		this->bytes_remaining = ntohll(n_length);
		this->message_length = this->bytes_remaining;

		// Check overflow
		if(p.size() < 10)
			return;

		uint16_t n_channel_length;
		std::memcpy(&n_channel_length, p.data()+8, 2);
		uint16_t channel_length = ntohs(n_channel_length);

		// Check overflow
		if((uint16_t)p.size() < 10 + channel_length)
			return;

		this->channel = std::string(p.data()+10, p.data()+10+channel_length);

		p.cover(10 + channel_length);
	}

	// Check if full message has been received
	if(bytes_remaining > p.size()) { // Incomplete message
		message_buffer.push_back(std::move(p));
		bytes_remaining -= p.size();
	} else { // Full message
		// Assemble final message
		std::unique_ptr<char[]> message(new char[message_length]);
		uint64_t offset = 0;
		for(
			auto iter = message_buffer.begin();
			iter != message_buffer.end();
			iter = message_buffer.erase(iter)
		) {
			std::memcpy(message.get() + offset, iter->data(), iter->size());
			offset += iter->size();
		}
		// Read only bytes_remaining bytes from final packet to prevent buffer overrun
		std::memcpy(message.get() + offset, p.data(), bytes_remaining);

		delegate.did_receive_message(std::move(message), message_length, this->channel);

		// Reset state. Message buffer should already be empty.
		bytes_remaining = 0;
		message_length = 0;
		channel.clear();
	}
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::send_MESSAGE(const net::SocketAddress &addr, std::string channel, const char *data, uint64_t size) {
	char *message = new char[channel.size()+11+size];

	message[0] = 3;

	uint64_t n_size = htonll(size);
	std::memcpy(message + 1, &n_size, 8);

	uint16_t n_channel_length = htons(channel.size());
	std::memcpy(message + 9, &n_channel_length, 2);
	std::memcpy(message + 11, channel.data(), channel.size());

	std::memcpy(message + 11 + channel.size(), data, size);

	std::unique_ptr<char[]> p(message);
	stream::StreamProtocol<PubSubNode>::send_data(*this, std::move(p), channel.size()+11+size, addr);
}

template<typename PubSubDelegate>
void PubSubNode<PubSubDelegate>::send_message_on_channel(std::string channel, const char *data, uint64_t size) {
	for (ListSocketAddress::iterator it = channel_subscription_map[channel].begin(); it != channel_subscription_map[channel].end(); it++) {
		spdlog::info("Sending message on channel {} to {}", channel, (*it).to_string());
		send_MESSAGE(*it, channel, data, size);
	}
}

} // namespace pubsub
} // namespace marlin

#endif // MARLIN_PUBSUB_PUBSUBNODE_HPP
