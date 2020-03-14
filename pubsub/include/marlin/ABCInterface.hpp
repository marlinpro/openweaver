/*
	1. loop to retry connection to contract interface on restart broken connection
	2. maps to store balance
	3. Null checks
	4. Acks
	5. Convert to server?
	6. int for balance
*/
#ifndef MARLIN_ABCInterface_HPP
#define MARLIN_ABCInterface_HPP

#include <fstream>
#include <experimental/filesystem>
#include <sodium.h>
#include <unistd.h>

#include <marlin/net/tcp/TcpTransportFactory.hpp>
#include <marlin/stream/StreamTransportFactory.hpp>
#include <marlin/lpf/LpfTransportFactory.hpp>
#include <marlin/net/core/EventLoop.hpp>

using namespace marlin;
using namespace marlin::net;
using namespace marlin::stream;
using namespace marlin::lpf;

std::string hexStr(char *data, int len)
{
	char* hexchar = new char[2*len + 1];
	int i;
	for (i=0; i<len; i++) {
		sprintf(hexchar+i*2, "%02hhx", data[i]);
	}
	hexchar[i*2] = 0;

	std::string hexstring = std::string(hexchar);
	delete[] hexchar;

	return hexstring;
}

const uint8_t addressLength = 32;
const uint8_t balanceLength = 32;
const uint8_t messageTypeSize = 1;
const uint8_t AckTypeSize = 1;
const uint8_t blockNumberSize = 8;
const uint64_t staleThreshold = 30000;

class ABCInterface;

const bool enable_cut_through = false;
using LpfTcpTransportFactory = LpfTransportFactory<
	ABCInterface,
	ABCInterface,
	TcpTransportFactory,
	TcpTransport,
	enable_cut_through
>;
using LpfTcpTransport = LpfTransport<
	ABCInterface,
	TcpTransport,
	enable_cut_through
>;

class ABCInterface {

std::map<std::string, std::string> stakeAddressMap;
uint64_t lastUpdateTime;
uint64_t latestBlockReceived;

public:
	LpfTcpTransportFactory f;
	LpfTcpTransport* contractInterface = nullptr;
	// timestamp
	// db

	ABCInterface() {
		f.bind(net::SocketAddress());
		dial(net::SocketAddress::from_string("127.0.0.1:7000"), NULL);
		lastUpdateTime = 0;
		latestBlockReceived = -1;
	}

	//-----------------------delegates for Lpf-Tcp-Transport-----------------------------------

	// Listen delegate: Not required
	bool should_accept(SocketAddress const &addr __attribute__((unused))) {
		return true;
	}

	void did_create_transport(LpfTcpTransport &transport) {
		SPDLOG_DEBUG(
			"DID CREATE LPF TRANSPORT: {}",
			transport.dst_addr.to_string()
		);

		transport.setup(this, NULL);

		contractInterface = &transport;
	}

	// Transport delegate

	// TODO?
	void did_dial(LpfTcpTransport &transport __attribute__((unused))) {
		SPDLOG_DEBUG(
			"DID DIAL: {}",
			transport.dst_addr.to_string()
		);
	}

	enum MessageType {
		StateUpdateMessage = 1,
		AckMessage = 2
	};

	// TODO Size checks and null checks while reading messages from buffer
	int did_recv_message(
		LpfTcpTransport &transport __attribute__((unused)),
		Buffer &&message  __attribute__((unused))
	) {
		SPDLOG_INFO(
			"Did recv from blockchain client, message with length {}",
			message.size()
		);

		auto messageType = message.read_uint8(0);
		message.cover(1);

		switch (messageType) {
			case MessageType::StateUpdateMessage:

				auto blockNumber = message.read_uint64_be(0);
				message.cover(8);

				if (blockNumber > latestBlockReceived) {
					auto numMapEntries = message.read_uint32_be(0);
					message.cover(4);

					SPDLOG_INFO(
						"BlockNumber {}",
						blockNumber
					);

					SPDLOG_INFO(
						"NumEntries {}",
						numMapEntries
					);

					for (uint i=0; i<numMapEntries; i++) {

						std::string addressString = hexStr(message.data(), addressLength);
						message.cover(addressLength);

						std::string balanceString = hexStr(message.data(), balanceLength);
						message.cover(balanceLength);

						stakeAddressMap[addressString] = balanceString;

						SPDLOG_INFO(
							"address {}",
							addressString
						);

						SPDLOG_INFO(
							"balance {}",
							balanceString
						);
					}

					latestBlockReceived = blockNumber;
					lastUpdateTime = EventLoop::now();
				}

				send_ack_state_update(transport, latestBlockReceived);
		}

		return 0;
	}

	void send_ack_state_update(
		LpfTcpTransport &transport __attribute__((unused)),
		uint64_t blockNumber) {

		auto messageType = MessageType::AckMessage;
		uint8_t ackType = MessageType::StateUpdateMessage;

		uint32_t totalSize = messageTypeSize + AckTypeSize + blockNumberSize;
		auto dataBuffer = new Buffer(new char[totalSize], totalSize);
		uint32_t offset = 0;

		dataBuffer->write_uint8(offset, messageType);
		offset += messageTypeSize;
		dataBuffer->write_uint8(offset, ackType);
		offset += AckTypeSize;
		dataBuffer->write_uint64_be(offset, blockNumber);
		offset += blockNumberSize;

		transport.send(std::move(*dataBuffer));
	}

	void did_send_message(
		LpfTcpTransport &transport __attribute__((unused)),
		Buffer &&message __attribute__((unused))
	) {}

	// TODO:
	void did_close(LpfTcpTransport &transport  __attribute__((unused))) {
		SPDLOG_DEBUG(
			"Closed connection with client: {}",
			transport.dst_addr.to_string()
		);

		contractInterface = nullptr;
	}

	int dial(SocketAddress const &addr, uint8_t const *remote_static_pk) {
		SPDLOG_INFO(
			"SENDING DIAL TO: {}",
			addr.to_string()
		);

		return f.dial(addr, *this, remote_static_pk);
	};


	void send_message(
		std::string channel __attribute__((unused)),
		uint64_t message_id __attribute__((unused)),
		const char *data __attribute__((unused))
	) {}

	// Enquiry calls by
	//checkBalance(xyz)
	bool is_alive() {
 		// return (EventLoop::now() - lastUpdateTime) > staleThreshold;
		return true;
	}

	// TODO check if entry is new
	std::string getStake(std::string address) {
		if (! is_alive() || (stakeAddressMap.find(address) == stakeAddressMap.end()))
			return "";
		return stakeAddressMap[address];
	}
};

#endif // MARLIN_ABCInterface_HPP
