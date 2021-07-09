#include "gtest/gtest.h"
#include "marlin/asyncio/udp/UdpFiber.hpp"
#include <marlin/core/fabric/Fabric.hpp>
#include <marlin/core/fibers/VersioningFiber.hpp>

#include <functional>

using namespace marlin::core;
using namespace marlin::asyncio;

struct Terminal {
	static constexpr bool is_outer_open = false;
	static constexpr bool is_inner_open = false;

	using InnerMessageType = Buffer;
	using OuterMessageType = Buffer;

	bool *received;
	uv_udp_t *loop;

	template<typename... Args>
	Terminal(Args&&...) {}

	Terminal(std::tuple <bool*> &&init_tuple) :
		received(std::get<0>(init_tuple)) {}

	int did_recv(auto&&, Buffer&& buf, SocketAddress addr) {
		SPDLOG_INFO("Terminal: Did recv: {} bytes from {}", buf.size(), addr.to_string());
		*received = true;
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

TEST(UdpFiber, FiberProperty) {
	bool *received = (bool*)malloc(sizeof *received);
	*received = true;

	Fabric<
		Terminal,
		UdpFiber,
		VersioningFiber
	> f(std::make_tuple(
		// terminal
		std::make_tuple(received),
		// udp fiber
		std::make_tuple(),
		std::make_tuple()
	));

	f.o(this).did_recv(0, Buffer(10), SocketAddress::from_string("0.0.0.0:8000"));
	EXPECT_EQ(*received, true);
}

void close_cb(uv_handle_t* handle) {
	delete handle;
}

TEST(UdpFiber, CanDialListen) {
	// std::vector <std::string> *indices = new std::vector <std::string>();

	bool *received = (bool*)malloc(sizeof *received);
	*received = false;

	Fabric<
		Terminal,
		UdpFiber,
		VersioningFiber
	> server(std::make_tuple(
		// terminal
		std::make_tuple(received),
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
		std::make_tuple(received),
		// udp fiber
		std::make_tuple(),
		std::make_tuple()
	));
	(void)client.i(server).bind(SocketAddress::from_string("127.0.0.1:9000"));
	(void)client.i(server).dial(SocketAddress::from_string("127.0.0.1:8000"));
	uv_run(uv_default_loop(), UV_RUN_NOWAIT);

	EXPECT_TRUE(*received);
}