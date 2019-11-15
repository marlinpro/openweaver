#include <marlin/net/udp/UdpTransportFactory.hpp>
#include <marlin/stream/StreamTransportFactory.hpp>
#include <uv.h>
#include <spdlog/spdlog.h>

using namespace marlin::net;
using namespace marlin::stream;

struct Delegate;

using TransportType = StreamTransport<Delegate, UdpTransport>;

#define SIZE 100000000

struct Delegate {
	void did_recv_bytes(
		TransportType &transport __attribute__((unused)),
		Buffer &&packet __attribute__((unused))
	) {
		// SPDLOG_INFO(
		// 	"Transport {{ Src: {}, Dst: {} }}: Did recv packet: {} bytes",
		// 	transport.src_addr.to_string(),
		// 	transport.dst_addr.to_string(),
		// 	packet.size()
		// );
	}

	void did_send_bytes(TransportType &transport, Buffer &&packet) {
		SPDLOG_INFO(
			"Transport {{ Src: {}, Dst: {} }}: Did send packet: {} bytes",
			transport.src_addr.to_string(),
			transport.dst_addr.to_string(),
			packet.size()
		);
		did_dial(transport);
	}

	void did_dial(TransportType &transport) {
		auto buf = Buffer(new char[SIZE], SIZE);

		SPDLOG_INFO("Did dial");

		transport.send(std::move(buf));
	}

	void did_close(TransportType &) {}

	bool should_accept(SocketAddress const &) {
		return true;
	}

	void did_create_transport(TransportType &transport) {
		transport.setup(this);
	}
};

int main() {
	StreamTransportFactory<
		Delegate,
		Delegate,
		UdpTransportFactory,
		UdpTransport
	> s, c;
	Delegate d;

	s.bind(SocketAddress::loopback_ipv4(8000));
	s.listen(d);
	c.bind(SocketAddress::loopback_ipv4(0));
	c.dial(SocketAddress::loopback_ipv4(8000), d);

	return uv_run(uv_default_loop(), UV_RUN_DEFAULT);
}
