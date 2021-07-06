#include <gtest/gtest.h>
#include "marlin/core/fabric/Fabric.hpp"

#include <cstring>
#include <string>

using namespace marlin::core;

// namespace marlin {
// namespace core {

// }
// }

struct Terminal {
	static constexpr bool is_outer_open = false;
	static constexpr bool is_inner_open = false;

	using InnerMessageType = Buffer;
	using OuterMessageType = Buffer;

	// template<typename... Args>
	// Terminal(Args&&... args) {}

	int idx;
	std::vector <std::string> *indices;

	Terminal(std::tuple <std::tuple<int, std::vector <std::string>*>> &&init_tuple) :
		idx(std::get<0>(std::get<0> (init_tuple))),
		indices(std::get<1>(std::get<0>(init_tuple))) {}

	int did_recv(auto&&, Buffer&&) {
		(*indices).push_back("t_dr");
		return 0;
	}

	template<typename FiberType>
	int send(FiberType&, Buffer&&, SocketAddress) {
		(*indices).push_back("t_s");
		return 0;
	}
};

template<typename ExtFabric>
struct Fiber {
	static constexpr bool is_outer_open = true;
	static constexpr bool is_inner_open = true;

	using InnerMessageType = Buffer;
	using OuterMessageType = Buffer;

	[[no_unique_address]] ExtFabric ext_fabric;
	int idx = 0;

	std::vector <std::string> *indices;

	template<typename ExtTupleType>
	Fiber(std::tuple<ExtTupleType, std::tuple<int, std::vector <std::string>*>>&& init_tuple) :
		ext_fabric(std::get<0>(init_tuple)),
		idx(std::get<0>(std::get<1>(init_tuple))),
		indices(std::get<1>(std::get<1>(init_tuple))) 
		{}

	template<typename ExtTupleType>
	Fiber(std::tuple<ExtTupleType, int>&& init_tuple) :
		ext_fabric(std::get<0>(init_tuple)),
		idx(std::get<1>(init_tuple)) {}

	int did_recv(auto&&, Buffer&& buf) {
		(*indices).push_back("f_dr_" + std::to_string(idx));
		return ext_fabric.i(*this).did_recv(*this, std::move(buf));
	}

	int send(auto&&, InnerMessageType &&buf, SocketAddress addr) {
		(*indices).push_back("f_s_" + std::to_string(idx));
		return ext_fabric.o(*this).send(*this, std::move(buf), addr);
	}

};

template<typename ExtFabric>
struct FiberOuterClose {
	static constexpr bool is_outer_open = false;
	static constexpr bool is_inner_open = true;

	using InnerMessageType = Buffer;
	using OuterMessageType = Buffer;

	[[no_unique_address]] ExtFabric ext_fabric;
	int idx = 0;
	std::vector <std::string> *indices;

	template<typename ExtTupleType>
	FiberOuterClose(std::tuple<ExtTupleType, std::tuple<int, std::vector <std::string>*>>&& init_tuple) :
		ext_fabric(std::get<0>(init_tuple)),
		idx(std::get<0>(std::get<1>(init_tuple))),
		indices(std::get<1>(std::get<1>(init_tuple))) 
		{}

	template<typename ExtTupleType>
	FiberOuterClose(std::tuple<ExtTupleType, int>&& init_tuple) :
		ext_fabric(std::get<0>(init_tuple)),
		idx(std::get<1>(init_tuple)) {}


	template <typename... Args>
	int dial(auto&&, SocketAddress, Args&& ...) {
		(*indices).push_back("foc_d_" + std::to_string(idx));
		return 0;
	}

	int bind(auto&&, SocketAddress) {
		(*indices).push_back("foc_b_" + std::to_string(idx));
		return 0;
	}

	int listen(auto&&) {
		(*indices).push_back("foc_l_" + std::to_string(idx));
		return 0;
	}
};



TEST(FabricTest, MessageOrder1) {
	std::vector <std::string> *indices = new std::vector <std::string>();
	Fabric <
		Terminal, 
		Fiber
	> f(std::make_tuple(
		std::make_tuple(std::make_tuple(-1, indices)),
		std::make_tuple(std::make_tuple(1, indices))
	));
	f.i(0).did_recv(0, Buffer(5));
	EXPECT_EQ(*indices, std::vector <std::string> ({"f_dr_1", "t_dr"}));
}

TEST(FabricTest, MessageOrder2) {
	std::vector <std::string> *indices = new std::vector <std::string>();
	Fabric<
		Terminal,
		Fiber,
		Fiber,
		Fiber,
		Fiber,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(4, indices)),
		std::make_tuple(std::make_tuple(5, indices))
	));
	f.i(0).did_recv(0, Buffer(5));
	EXPECT_EQ(*indices, std::vector <std::string>({"f_dr_1", "f_dr_2", "f_dr_3", "f_dr_4", "f_dr_5", "t_dr"}));
}

TEST(FabricTest, MessageOrder3) {
	std::vector <std::string> *indices = new std::vector <std::string>();
	Fabric<	
		Terminal,
		FabricF<Fiber, Fiber>::type
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(
			std::make_tuple(1, indices)
		), std::make_tuple(
			std::make_tuple(2, indices)
		))
	));
	f.i(0).did_recv(0, Buffer(5));
	EXPECT_EQ(*indices, std::vector <std::string> ({"f_dr_1", "f_dr_2", "t_dr"}));
}

TEST(FabricTest, MessageOrder4) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<
		Terminal,
		Fiber,
		Fiber,
		Fiber,
		Fiber,
		FabricF<Fiber, Fiber>::type
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(4, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices)))
	));
	f.i(0).did_recv(0, Buffer(5));
	EXPECT_EQ(*indices, std::vector <std::string> ({"f_dr_1", "f_dr_2", "f_dr_3", "f_dr_4", "f_dr_1", "f_dr_2", "t_dr"}));
}

TEST(FabricTest, MessageOrder5) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<
		Terminal,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(4, indices))
	));
	f.i(0).did_recv(0, Buffer(5)); 
	EXPECT_EQ(*indices, std::vector <std::string> ({"f_dr_1", "f_dr_1", "f_dr_2", "f_dr_2", "f_dr_1", "f_dr_2", "f_dr_3", "f_dr_1", "f_dr_2", "f_dr_4", "t_dr"}));
}

TEST(FabricTest, sendFunction) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<
		Terminal,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(4, indices))
	));
	f.o(0).send(0, Buffer(5), SocketAddress::from_string("0.0.0.0:3000"));
	EXPECT_EQ(*indices, std::vector <std::string> ({"f_s_4", "f_s_2", "f_s_1", "f_s_3", "f_s_2", "f_s_1", "f_s_2", "f_s_2", "f_s_1", "f_s_1", "t_s"}));
}

TEST(FabricTest, dialFunction) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<	
		Terminal,
		FiberOuterClose,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)),
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)),
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(4, indices))
	));
	f.i(0).dial(0, SocketAddress::from_string("0.0.0.0:3000"), Buffer(5));
	EXPECT_EQ(*indices, std::vector <std::string> ({"foc_d_1"}));
}


TEST(FabricTest, bindFunction) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<	
		Terminal,
		FiberOuterClose,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(4, indices))
	));
	f.i(0).bind(0, SocketAddress::from_string("0.0.0.0:3000"));
	EXPECT_EQ(*indices, std::vector <std::string> ({"foc_b_1"}));
}

TEST(FabricTest, listenFunction) {
	std::vector <std::string> *indices = new std::vector <std::string> ();
	Fabric<	
		Terminal,
		FiberOuterClose,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber,
		FabricF<Fiber, Fiber>::type,
		Fiber
	> f(std::make_tuple(
		// Terminal
		std::make_tuple(std::make_tuple(-1, indices)),
		// Other fibers
		std::make_tuple(std::make_tuple(1, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(2, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(3, indices)),
		std::make_tuple(std::make_tuple(std::make_tuple(1, indices)), 
						std::make_tuple(std::make_tuple(2, indices))),
		std::make_tuple(std::make_tuple(4, indices))
	));
	f.i(0).listen(0);
	EXPECT_EQ(*indices, std::vector <std::string> ({"foc_l_1"}));
}
