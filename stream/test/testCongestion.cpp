#include <marlin/asyncio/udp/UdpTransportFactory.hpp>
#include <marlin/stream/StreamTransportFactory.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bin_to_hex.h>
#include "gtest/gtest.h"

using namespace marlin::core;
using namespace marlin::asyncio;
using namespace marlin::stream;

struct Delegate;

using TransportType = StreamTransport<Delegate, UdpTransport>;

#define s_SIZE 10000

uint8_t static_sk[crypto_box_SECRETKEYBYTES];
uint8_t static_pk[crypto_box_PUBLICKEYBYTES];

struct Delegate {

    std::function<void(TransportType &, Buffer &&)> t_did_send;

	int did_recv(
		TransportType &transport [[maybe_unused]],
		Buffer &&packet [[maybe_unused]],
		uint8_t stream_id [[maybe_unused]]
	) {
		 SPDLOG_INFO(
		 	"Transport {{ Src: {}, Dst: {} }}: Did recv packet: {} bytes",
		 	transport.src_addr.to_string(),
		 	transport.dst_addr.to_string(),
		 	packet.size()
		 );
		return 0;
	}

	void did_send(TransportType &transport, Buffer &&packet) {
        t_did_send(transport, std::move(packet));
	}

	void did_dial(TransportType &transport) {
		auto buf = Buffer(s_SIZE);
		std::memset(buf.data(), 0, s_SIZE);
		SPDLOG_INFO("Did dial");
		transport.send(std::move(buf));
	}

	void did_close(TransportType &, uint16_t) {
	}

	bool should_accept(SocketAddress const &) {
		return true;
	}

	void did_create_transport(TransportType &transport) {
		transport.setup(this, static_sk);
	}

	void did_recv_flush_stream(
		TransportType &transport [[maybe_unused]],
		uint16_t stream_id [[maybe_unused]],
		uint64_t offset [[maybe_unused]],
		uint64_t old_offset [[maybe_unused]]
	) {
	}

	void did_recv_skip_stream(
		TransportType &transport [[maybe_unused]],
		uint16_t stream_id [[maybe_unused]]
	) {
	}

	void did_recv_flush_conf(
		TransportType &transport [[maybe_unused]],
		uint16_t stream_id [[maybe_unused]]
	) {
	}
};

TEST(StreamCongestion, Constructible) {

    	crypto_box_keypair(static_pk, static_sk);

    	SPDLOG_INFO(
	    	"PK: {:spn}\nSK: {:spn}",
		    spdlog::to_hex(static_pk, static_pk+32),
		    spdlog::to_hex(static_sk, static_sk+32)
	    );

	    StreamTransportFactory<
	    	Delegate,
		    Delegate,
		    UdpTransportFactory,
		    UdpTransport
	    > s, c;

	    Delegate d;
        d.t_did_send = [&] (TransportType &transport, Buffer &&packet)
        {
            SPDLOG_INFO(
        	    "Transport {{ Src: {}, Dst: {} }}: Did send packet: {} bytes",
        	    transport.src_addr.to_string(),
        	    transport.dst_addr.to_string(),
        	    packet.size()
            );
        };

	    s.bind(SocketAddress::loopback_ipv4(8000));
	    s.listen(d);
	    c.bind(SocketAddress::loopback_ipv4(0));
	    c.dial(SocketAddress::loopback_ipv4(8000), d, static_pk);

	    EXPECT_EQ(1,1);
}