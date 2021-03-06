#include "marlin/asyncio/udp/UdpFiber.hpp"
#include <marlin/core/fabric/Fabric.hpp>
#include <marlin/core/fibers/VersioningFiber.hpp>

#include <spdlog/spdlog.h>

using namespace marlin::core;
using namespace marlin::asyncio;

struct Terminal {
	static constexpr bool is_outer_open = false;
	static constexpr bool is_inner_open = false;

	using InnerMessageType = Buffer;
	using OuterMessageType = Buffer;

	template<typename... Args>
	Terminal(Args&&...) {}

	int did_recv(auto&&, Buffer&& buf, SocketAddress addr) {
		SPDLOG_INFO("Terminal: Did recv: {} bytes from {}", buf.size(), addr.to_string());
		return 0;
	}

	template<typename FiberType>
	int did_dial(FiberType& fabric, SocketAddress addr) {
		SPDLOG_INFO("Terminal: Did dial: {}", addr.to_string());
		fabric.o(*this).send(0, Buffer({0,0,0,0,0}, 5), addr);
		return 0;
	}

	template<typename FiberType>
	int did_send(FiberType&, Buffer&& buf) {
		SPDLOG_INFO("Terminal: Did send: {} bytes", buf.size());
		return 0;
	}
};


int main() {
	Fabric<
		Terminal,
		UdpFiber,
		VersioningFiber
	> server(std::make_tuple(
		// terminal
		std::make_tuple(),
		// udp fiber
		std::make_tuple(),
		std::make_tuple()
	));
	(void)server.i(server).bind(SocketAddress::from_string("127.0.0.1:8000"));
	(void)server.i(server).listen();

	Fabric<
		Terminal,
		UdpFiber,
		VersioningFiber
	> client(std::make_tuple(
		// terminal
		std::make_tuple(),
		// udp fiber
		std::make_tuple(),
		std::make_tuple()
	));
	(void)client.i(server).bind(SocketAddress::from_string("127.0.0.1:9000"));
	(void)client.i(server).dial(SocketAddress::from_string("127.0.0.1:8000"));

	return uv_run(uv_default_loop(), UV_RUN_DEFAULT);
}
