#ifndef MARLIN_STREAM_STREAMTRANSPORT_HPP
#define MARLIN_STREAM_STREAMTRANSPORT_HPP

#include <spdlog/spdlog.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <unordered_set>
#include <unordered_map>
#include <random>
#include <utility>

#include <sodium.h>

#include <marlin/core/SocketAddress.hpp>
#include <marlin/core/Buffer.hpp>
#include <marlin/asyncio/core/EventLoop.hpp>
#include <marlin/asyncio/core/Timer.hpp>
#include <marlin/core/TransportManager.hpp>

#include "protocol/SendStream.hpp"
#include "protocol/RecvStream.hpp"
#include "protocol/AckRanges.hpp"
#include "Messages.hpp"

namespace marlin {
namespace stream {

/// Timeout when no acks are received, used by the TLP timer
#define DEFAULT_TLP_INTERVAL 1000
/// Bytes that can be sent in a given batch, used by the packet pacing mechanism
#define DEFAULT_PACING_LIMIT 400000
/// Bytes that can be sent in a single packet to prevent fragmentation, accounts for header overheads
#define DEFAULT_FRAGMENT_SIZE 1350

/// @brief Transport class which provides stream semantics.
///
/// Wraps around a base transport providing datagram semantics.
///
/// Features:
/// \li In-order delivery
/// \li Reliable delivery
/// \li Congestion control
/// \li Transport layer encryption (disabled by default)
/// \li Stream multiplexing
/// \li No head-of-line blocking
template<typename DelegateType, template<typename> class DatagramTransport>
class StreamTransport {
private:
	static constexpr bool is_encrypted = false;

	/// Self type
	using Self = StreamTransport<DelegateType, DatagramTransport>;
	/// Base transport type
	using BaseTransport = DatagramTransport<Self>;
	/// Base message type
	using BaseMessageType = typename BaseTransport::MessageType;

	/// DATA message type
	using DATA = DATAWrapper<BaseMessageType>;
	/// ACK message type
	using ACK = ACKWrapper<BaseMessageType>;
	/// DIAL message type
	using DIAL = DIALWrapper<BaseMessageType>;
	/// DIALCONF message type
	using DIALCONF = DIALCONFWrapper<BaseMessageType>;
	/// CONF message type
	using CONF = CONFWrapper<BaseMessageType>;
	/// RST message type
	using RST = RSTWrapper<BaseMessageType>;
	/// SKIPSTREAM message type
	using SKIPSTREAM = SKIPSTREAMWrapper<BaseMessageType>;
	/// FLUSHSTREAM message type
	using FLUSHSTREAM = FLUSHSTREAMWrapper<BaseMessageType>;
	/// FLUSHCONF message type
	using FLUSHCONF = FLUSHCONFWrapper<BaseMessageType>;
	/// CLOSE message type
	using CLOSE = CLOSEWrapper<BaseMessageType>;
	/// CLOSECONF message type
	using CLOSECONF = CLOSECONFWrapper<BaseMessageType>;

	/// Base transport instance
	BaseTransport &transport;
	/// Transport manager of self
	core::TransportManager<Self> &transport_manager;

	/// Reset the transport's state (connection state, timers, streams, data queues, buffers, etc)
	void reset();

	/// List of possible connection states
	enum struct ConnectionState {
		Listen,
		DialSent,
		DialRcvd,
		Established,
		Closing
	} conn_state = ConnectionState::Listen;

	/// Src connection id
	uint32_t src_conn_id = 0;
	/// Dst connection id
	uint32_t dst_conn_id = 0;

	/// Timer instance to handle timeouts in the ConnectionState state machine
	asyncio::Timer state_timer;
	/// Timer interval for the state timer
	/// Can be used to handle things like exponential backoff
	uint64_t state_timer_interval = 0;

	/// Did we initiate the connection by dialling?
	bool dialled = false;
	/// Timer callback for handling DIAL timeouts
	void dial_timer_cb();

	// Streams
	/// List of streams on which we send data
	std::unordered_map<uint16_t, SendStream> send_streams;
	/// List of streams from which we recv data
	std::unordered_map<uint16_t, RecvStream> recv_streams;

	/// Helper function to get a send stream of given stream id, creating one if needed
	SendStream &get_or_create_send_stream(uint16_t stream_id);
	/// Helper function to get a recv stream of given stream id, creating one if needed
	RecvStream &get_or_create_recv_stream(uint16_t stream_id);

	// Packets
	/// Packet number of last sent packet.
	/// Strictly increasing, retransmitted packets have different packet number than the original
	uint64_t last_sent_packet = -1;
	/// List of sent packets which have not been acked yet
	std::map<uint64_t, SentPacketInfo> sent_packets;

	/// List of packets marked as lost.
	/// Can happen if packets sent much later were acknowledged.
	/// Can happen if an ack is not received for a long time.
	std::map<uint64_t, SentPacketInfo> lost_packets;

	// RTT estimate
	/// RTT estimate of connection
	double rtt = -1;

	// Congestion control
	uint64_t bytes_in_flight = 0;
	uint64_t k = 0;
	uint64_t w_max = 0;
	uint64_t congestion_window = 100000;
	uint64_t ssthresh = -1;
	uint64_t congestion_start = 0;
	uint64_t largest_acked = 0;
	uint64_t largest_sent_time = 0;

	// Send
	/// List of stream ids with data ready to be sent
	std::unordered_set<uint16_t> send_queue_ids;
	/// List of send streams with data ready to be sent
	std::list<SendStream *> send_queue;

	/// Add the given stream to the list of streams with data ready to be sent
	bool register_send_intent(SendStream &stream);

	/// Send any pending data that needs to be sent.
	/// Main entry point which keeps the transmission moving forward.
	/// First retransmits any lost data, then any new data.
	/// Packets are paced using the pacing timer.
	void send_pending_data();
	/// Send any lost data if possible
	int send_lost_data(uint64_t initial_bytes_in_flight);
	/// Send any new data if possible
	int send_new_data(SendStream &stream, uint64_t initial_bytes_in_flight);

	// Pacing
	/// Timer to enforce packet pacing
	asyncio::Timer pacing_timer;
	/// Is the pacing timer active?
	bool is_pacing_timer_active = false;
	/// Pacing timer callback to send a new batch of packets
	void pacing_timer_cb();

	// TLP (Tail Loss Probe)
	/// Timer to detect no acks for a long time
	asyncio::Timer tlp_timer;
	/// Timer interval for the tlp timer
	uint64_t tlp_interval = DEFAULT_TLP_INTERVAL;
	/// Timer callback for handling tlp timeouts
	void tlp_timer_cb();

	// ACKs
	/// Stores ranges of packet numbers that have and haven't been seen
	AckRanges ack_ranges;
	/// Timer to batch acks for multiple packets
	asyncio::Timer ack_timer;
	/// Is the ack timer active?
	bool ack_timer_active = false;
	/// Timer callback for sending an ack
	void ack_timer_cb();

	// Protocol
	void send_DIAL();
	void did_recv_DIAL(DIAL &&packet);

	void send_DIALCONF();
	void did_recv_DIALCONF(DIALCONF &&packet);

	void send_CONF();
	void did_recv_CONF(CONF &&packet);

	void send_RST(uint32_t src_conn_id, uint32_t dst_conn_id);
	void did_recv_RST(RST &&packet);

	void send_DATA(
		SendStream &stream,
		DataItem &data_item,
		uint64_t offset,
		uint16_t length
	);
	void did_recv_DATA(DATA &&packet);

	void send_ACK();
	void did_recv_ACK(ACK &&packet);

	void send_SKIPSTREAM(uint16_t stream_id, uint64_t offset);
	void did_recv_SKIPSTREAM(SKIPSTREAM &&packet);

	void send_FLUSHSTREAM(uint16_t stream_id, uint64_t offset);
	void did_recv_FLUSHSTREAM(FLUSHSTREAM &&packet);

	void send_FLUSHCONF(uint16_t stream_id);
	void did_recv_FLUSHCONF(FLUSHCONF &&packet);

	void send_CLOSE(uint16_t reason);
	void did_recv_CLOSE(CLOSE &&packet);

	void send_CLOSECONF(uint32_t src_conn_id, uint32_t dst_conn_id);
	void did_recv_CLOSECONF(CLOSECONF &&packet);

public:
	/// Delegate calls from base transport
	void did_dial(BaseTransport &transport, uint8_t const* remote_static_pk);
	/// Delegate calls from base transport
	void did_recv(BaseTransport &transport, BaseMessageType &&packet);
	/// Delegate calls from base transport
	void did_send(BaseTransport &transport, core::Buffer &&packet);
	/// Delegate calls from base transport
	void did_close(BaseTransport &transport, uint16_t reason);

	/// Own address
	core::SocketAddress src_addr;
	/// Destination address
	core::SocketAddress dst_addr;

	/// Delegate to notify of any events
	DelegateType *delegate = nullptr;

	/// Constructor
	StreamTransport(
		core::SocketAddress const &src_addr,
		core::SocketAddress const &dst_addr,
		BaseTransport &transport,
		core::TransportManager<Self> &transport_manager
	);

	/// Setup function that can be called to set the delegate and the private key
	void setup(DelegateType *delegate, uint8_t const* static_sk);
	/// Queues the given buffer for transmission
	int send(core::Buffer &&bytes, uint16_t stream_id = 0);

	/// Close reason
	uint16_t close_reason = 0;
	/// Closes the transport, ignores any further data received or queued
	void close(uint16_t reason = 0);
	/// Timer callback for close conf timeout
	void close_timer_cb();

	/// Is the transport ready to send data?
	bool is_active();
	/// Get the RTT estimate of the connection
	double get_rtt();

	/// Timer callback for SKIPSTREAM timeout
	void skip_timer_cb(RecvStream& stream);
	/// Ask the sender to skip to the end of the current transmission
	/// Can be used to skip receiving data we already have
	void skip_stream(uint16_t stream_id);

	/// Timer callback for FLUSHSTREAM timeout
	void flush_timer_cb(SendStream& stream);
	/// Tells the receiver that we are skipping to the end of the current transmission
	/// Can be used to skip sending data the receiver already has
	/// Can be used to skip sending data that we sent partially but can't send anymore
	void flush_stream(uint16_t stream_id);

	bool is_internal();

private:
	uint8_t static_sk[crypto_box_SECRETKEYBYTES];
	uint8_t static_pk[crypto_box_PUBLICKEYBYTES];

	uint8_t remote_static_pk[crypto_box_PUBLICKEYBYTES];

	uint8_t ephemeral_sk[crypto_kx_SECRETKEYBYTES];
	uint8_t ephemeral_pk[crypto_kx_PUBLICKEYBYTES];

	uint8_t remote_ephemeral_pk[crypto_kx_PUBLICKEYBYTES];

	uint8_t rx[crypto_kx_SESSIONKEYBYTES];
	uint8_t tx[crypto_kx_SESSIONKEYBYTES];

	uint8_t nonce[crypto_aead_aes256gcm_NPUBBYTES];

	alignas(16) crypto_aead_aes256gcm_state rx_ctx;
	alignas(16) crypto_aead_aes256gcm_state tx_ctx;
public:
	/// Get the public key of self
	uint8_t const* get_static_pk();
	/// Get the public key of the destination
	uint8_t const* get_remote_static_pk();
};


// Impl

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::reset() {
	// Reset transport
	conn_state = ConnectionState::Listen;
	src_conn_id = 0;
	dst_conn_id = 0;
	dialled = false;
	state_timer.stop();
	state_timer_interval = 0;

	for(auto& [_, stream] : send_streams) {
		(void)_;
		stream.state_timer.stop();
	}
	send_streams.clear();
	for(auto& [_, stream] : recv_streams) {
		(void)_;
		stream.state_timer.stop();
	}
	recv_streams.clear();

	last_sent_packet = -1;
	sent_packets.clear();
	lost_packets.clear();

	rtt = -1;

	bytes_in_flight = 0;
	k = 0;
	w_max = 0;
	congestion_window = 100000;
	ssthresh = -1;
	congestion_start = 0;
	largest_acked = 0;
	largest_sent_time = 0;

	send_queue_ids.clear();
	send_queue.clear();

	pacing_timer.stop();
	is_pacing_timer_active = false;

	tlp_timer.stop();
	tlp_interval = DEFAULT_TLP_INTERVAL;

	ack_ranges = AckRanges();
	ack_timer.stop();
	ack_timer_active = false;
}

// Impl

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::dial_timer_cb() {
	if(this->state_timer_interval >= 64000) { // Abort on too many retries
		this->state_timer_interval = 0;
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: Dial timeout",
			this->src_addr.to_string(),
			this->dst_addr.to_string()
		);
		reset();
		transport.close();
		return;
	}

	this->send_DIAL();
	this->state_timer_interval *= 2;
	this->state_timer.template start<Self, &Self::dial_timer_cb>(
		this->state_timer_interval,
		0
	);
}

//---------------- Stream functions begin ----------------//


template<typename DelegateType, template<typename> class DatagramTransport>
SendStream &StreamTransport<DelegateType, DatagramTransport>::get_or_create_send_stream(
	uint16_t stream_id
) {
	auto iter = send_streams.try_emplace(
		stream_id,
		stream_id,
		this
	).first;

	return iter->second;
}


template<typename DelegateType, template<typename> class DatagramTransport>
RecvStream &StreamTransport<DelegateType, DatagramTransport>::get_or_create_recv_stream(
	uint16_t stream_id
) {
	auto iter = recv_streams.try_emplace(
		stream_id,
		stream_id,
		this
	).first;

	return iter->second;
}

//---------------- Stream functions end ----------------//


//---------------- Send functions end ----------------//


template<typename DelegateType, template<typename> class DatagramTransport>
bool StreamTransport<DelegateType, DatagramTransport>::register_send_intent(
	SendStream &stream
) {
	if(send_queue_ids.find(stream.stream_id) != send_queue_ids.end()) {
		return false;
	}

	send_queue.push_back(&stream);
	send_queue_ids.insert(stream.stream_id);

	return true;
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_pending_data() {
	if(is_pacing_timer_active == false) {
		is_pacing_timer_active = true;
		pacing_timer.template start<Self, &Self::pacing_timer_cb>(0, 0);
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
int StreamTransport<DelegateType, DatagramTransport>::send_lost_data(
	uint64_t initial_bytes_in_flight
) {
	for(
		auto iter = lost_packets.begin();
		iter != lost_packets.end();
		iter = lost_packets.erase(iter)
	) {
		if(bytes_in_flight - initial_bytes_in_flight >= DEFAULT_PACING_LIMIT) {
			// Pacing limit hit, reschedule timer
			is_pacing_timer_active = true;
			pacing_timer.template start<Self, &Self::pacing_timer_cb>(1, 0);
			return -1;
		}

		auto &sent_packet = iter->second;
		if(bytes_in_flight > congestion_window - sent_packet.length) {
			return -2;
		}

		send_DATA(
			*sent_packet.stream,
			*sent_packet.data_item,
			sent_packet.offset,
			sent_packet.length
		);

		sent_packet.stream->bytes_in_flight += sent_packet.length;
		bytes_in_flight += sent_packet.length;

		SPDLOG_DEBUG("Lost packet sent: {}, {}", sent_packet.offset, last_sent_packet);
	}

	return 0;
}

template<typename DelegateType, template<typename> class DatagramTransport>
int StreamTransport<DelegateType, DatagramTransport>::send_new_data(
	SendStream &stream,
	uint64_t initial_bytes_in_flight
) {
	for(
		;
		stream.next_item_iterator != stream.data_queue.end();
		stream.next_item_iterator++
	) {
		auto &data_item = *stream.next_item_iterator;

		for(
			uint64_t i = data_item.sent_offset;
			i < data_item.data.size();
			i+=DEFAULT_FRAGMENT_SIZE
		) {
			auto remaining_bytes = data_item.data.size() - data_item.sent_offset;
			uint16_t dsize = remaining_bytes > DEFAULT_FRAGMENT_SIZE ? DEFAULT_FRAGMENT_SIZE : remaining_bytes;

			if(this->bytes_in_flight > this->congestion_window - dsize)
				return -2;

			if(this->bytes_in_flight - initial_bytes_in_flight > DEFAULT_PACING_LIMIT) {
				return -1;
			}

			send_DATA(stream, data_item, i, dsize);

			stream.bytes_in_flight += dsize;
			stream.sent_offset += dsize;
			this->bytes_in_flight += dsize;
			data_item.sent_offset += dsize;
		}
	}

	return 0;
}

//---------------- Send functions end ----------------//


//---------------- Pacing functions begin ----------------//

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::pacing_timer_cb() {
	this->is_pacing_timer_active = false;

	auto initial_bytes_in_flight = this->bytes_in_flight;

	auto res = this->send_lost_data(initial_bytes_in_flight);
	if(res < 0) {
		return;
	}

	// New packets
	for(
		auto iter = this->send_queue.begin();
		iter != this->send_queue.end();
		// Empty
	) {
		auto &stream = **iter;

		int res = this->send_new_data(stream, initial_bytes_in_flight);
		if(res == 0) { // Idle stream, move to next stream
			this->send_queue_ids.erase(stream.stream_id);
			iter = this->send_queue.erase(iter);
		} else if(res == -1) { // Pacing limit hit, reschedule timer
			// Pacing limit hit, reschedule timer
			this->is_pacing_timer_active = true;
			pacing_timer.template start<Self, &Self::pacing_timer_cb>(1, 0);
			return;
		} else { // Congestion window exhausted, break
			return;
		}
	}
}

//---------------- Pacing functions end ----------------//


//---------------- TLP functions begin ----------------//

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::tlp_timer_cb() {
	if(this->sent_packets.size() == 0 && this->lost_packets.size() == 0 && this->send_queue.size() == 0) {
		// Idle connection, stop timer
		tlp_timer.stop();
		this->tlp_interval = DEFAULT_TLP_INTERVAL;
		return;
	}

	SPDLOG_DEBUG("TLP timer: {}, {}, {}", this->sent_packets.size(), this->lost_packets.size(), this->send_queue.size() == 0);

	auto sent_iter = this->sent_packets.cbegin();

	// Retry lost packets
	// No condition necessary, all are considered lost if tail probe fails
	while(sent_iter != this->sent_packets.cend()) {
		this->bytes_in_flight -= sent_iter->second.length;
		sent_iter->second.stream->bytes_in_flight -= sent_iter->second.length;
		this->lost_packets[sent_iter->first] = sent_iter->second;

		sent_iter++;
	}

	if(sent_iter == this->sent_packets.cbegin()) {
		// No lost packets, ignore
	} else {
		// Lost packets, congestion event
		auto last_iter = std::prev(sent_iter);
		auto &sent_packet = last_iter->second;
		if(sent_packet.sent_time > this->congestion_start) {
			// New congestion event
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: Timer congestion event: {}",
				this->src_addr.to_string(),
				this->dst_addr.to_string(),
				this->congestion_window
			);
			this->congestion_start = asyncio::EventLoop::now();

			if(this->congestion_window < this->w_max) {
				// Fast convergence
				this->w_max = this->congestion_window;
				this->congestion_window *= 0.6;
			} else {
				this->w_max = this->congestion_window;
				this->congestion_window *= 0.75;
			}

			if(this->congestion_window < 10000) {
				this->congestion_window = 10000;
			}

			this->ssthresh = this->congestion_window;

			this->k = std::cbrt(this->w_max / 16)*1000;
		}

		// Pop lost packets from sent
		this->sent_packets.erase(this->sent_packets.cbegin(), sent_iter);
	}

	// New packets
	this->send_pending_data();

	// Next timer interval
	if(this->tlp_interval < 25000) {
		this->tlp_interval *= 2;
		this->tlp_timer.template start<Self, &Self::tlp_timer_cb>(this->tlp_interval, 0);
	} else {
		// Abort on too many retries
		SPDLOG_DEBUG("Lost peer: {}", this->dst_addr.to_string());
		reset();
		transport.close();
	}
}

//---------------- TLP functions end ----------------//


//---------------- ACK functions begin ----------------//

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::ack_timer_cb() {
	send_ACK();

	ack_timer_active = false;
}

//---------------- ACK functions end ----------------//


//---------------- Protocol functions begin ----------------//

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_DIAL() {
	SPDLOG_DEBUG(
		"Stream transport {{ Src: {}, Dst: {} }}: DIAL >>>> {:spn}",
		src_addr.to_string(),
		dst_addr.to_string(),
		spdlog::to_hex(remote_static_pk, remote_static_pk+crypto_box_PUBLICKEYBYTES)
	);

	constexpr size_t pt_len = crypto_box_PUBLICKEYBYTES + crypto_kx_PUBLICKEYBYTES;
	constexpr size_t ct_len = pt_len + crypto_box_SEALBYTES;

	uint8_t buf[ct_len];
	std::memcpy(buf, static_pk, crypto_box_PUBLICKEYBYTES);
	std::memcpy(buf + crypto_box_PUBLICKEYBYTES, ephemeral_pk, crypto_kx_PUBLICKEYBYTES);
	crypto_box_seal(buf, buf, pt_len, remote_static_pk);

	transport.send(
		DIAL(ct_len)
		.set_src_conn_id(this->src_conn_id)
		.set_dst_conn_id(this->dst_conn_id)
		.set_payload(buf, ct_len)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_DIAL(
	DIAL &&packet
) {
	constexpr size_t pt_len = (crypto_box_PUBLICKEYBYTES + crypto_kx_PUBLICKEYBYTES);
	constexpr size_t ct_len = pt_len + crypto_box_SEALBYTES;

	if(!packet.validate(ct_len)) {
		return;
	}

	switch(conn_state) {
	case ConnectionState::Listen: {
		if(packet.src_conn_id() != 0) { // Should have empty source
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Should have empty src: {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				packet.src_conn_id()
			);
			return;
		}

		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: DIAL <<<< {:spn}",
			src_addr.to_string(),
			dst_addr.to_string(),
			spdlog::to_hex(static_pk, static_pk+crypto_box_PUBLICKEYBYTES)
		);

		uint8_t pt[pt_len];
		auto res = crypto_box_seal_open(pt, packet.payload(), ct_len, static_pk, static_sk);
		if (res < 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Unseal failure: {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				res
			);
			return;
		}

		std::memcpy(remote_static_pk, pt, crypto_box_PUBLICKEYBYTES);
		std::memcpy(remote_ephemeral_pk, pt + crypto_box_PUBLICKEYBYTES, crypto_kx_PUBLICKEYBYTES);

		auto *kdf = (std::memcmp(ephemeral_pk, remote_ephemeral_pk, crypto_box_PUBLICKEYBYTES) > 0) ? &crypto_kx_server_session_keys : &crypto_kx_client_session_keys;

		if ((*kdf)(rx, tx, ephemeral_pk, ephemeral_sk, remote_ephemeral_pk) != 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Key derivation failure",
				src_addr.to_string(),
				dst_addr.to_string()
			);
			return;
		}

		randombytes_buf(nonce, crypto_aead_aes256gcm_NPUBBYTES);
		crypto_aead_aes256gcm_beforenm(&rx_ctx, rx);
		crypto_aead_aes256gcm_beforenm(&tx_ctx, tx);

		this->dst_conn_id = packet.dst_conn_id();
		this->src_conn_id = (uint32_t)std::random_device()();

		send_DIALCONF();

		conn_state = ConnectionState::DialRcvd;

		break;
	}

	case ConnectionState::DialSent: {
		if(packet.src_conn_id() != 0) { // Should have empty source
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Should have empty src: {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				packet.src_conn_id()
			);
			return;
		}

		uint8_t pt[pt_len];
		auto res = crypto_box_seal_open(pt, packet.payload() + 10, ct_len, static_pk, static_sk);
		if (res < 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Unseal failure: {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				res
			);
			return;
		}

		std::memcpy(remote_ephemeral_pk, pt + crypto_box_PUBLICKEYBYTES, crypto_kx_PUBLICKEYBYTES);

		auto *kdf = (std::memcmp(ephemeral_pk, remote_ephemeral_pk, crypto_box_PUBLICKEYBYTES) > 0) ? &crypto_kx_server_session_keys : &crypto_kx_client_session_keys;

		if ((*kdf)(rx, tx, ephemeral_pk, ephemeral_sk, remote_ephemeral_pk) != 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIAL: Key derivation failure",
				src_addr.to_string(),
				dst_addr.to_string()
			);
			return;
		}

		randombytes_buf(nonce, crypto_aead_aes256gcm_NPUBBYTES);
		crypto_aead_aes256gcm_beforenm(&rx_ctx, rx);
		crypto_aead_aes256gcm_beforenm(&tx_ctx, tx);

		this->dst_conn_id = packet.dst_conn_id();

		state_timer.stop();
		state_timer_interval = 0;

		send_DIALCONF();

		conn_state = ConnectionState::DialRcvd;

		break;
	}

	case ConnectionState::DialRcvd:
	case ConnectionState::Established: {
		// Send existing ids
		// Wait for dst to RST and try to establish again
		// if this connection is stale
		send_DIALCONF();

		break;
	}

	case ConnectionState::Closing: {
		// Ignore
		break;
	}
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_DIALCONF() {
	constexpr size_t pt_len = crypto_kx_PUBLICKEYBYTES;
	constexpr size_t ct_len = pt_len + crypto_box_SEALBYTES;

	uint8_t buf[ct_len];
	crypto_box_seal(buf, ephemeral_pk, pt_len, remote_static_pk);

	transport.send(
		DIALCONF(ct_len)
		.set_src_conn_id(this->src_conn_id)
		.set_dst_conn_id(this->dst_conn_id)
		.set_payload(buf, ct_len)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_DIALCONF(
	DIALCONF &&packet
) {
	constexpr size_t pt_len = crypto_kx_PUBLICKEYBYTES;
	constexpr size_t ct_len = pt_len + crypto_box_SEALBYTES;

	if(!packet.validate(ct_len)) {
		return;
	}

	switch(conn_state) {
	case ConnectionState::DialSent: {
		auto src_conn_id = packet.src_conn_id();
		if(src_conn_id != this->src_conn_id) {
			// On conn id mismatch, send RST for that id
			// Connection should ideally be reestablished by
			// dial retry sending another DIAL
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Src id mismatch: {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				src_conn_id,
				this->src_conn_id
			);
			send_RST(src_conn_id, packet.dst_conn_id());
			return;
		}

		if (crypto_box_seal_open(remote_ephemeral_pk, packet.payload(), ct_len, static_pk, static_sk) != 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Unseal failure",
				src_addr.to_string(),
				dst_addr.to_string()
			);
			return;
		}

		auto *kdf = (std::memcmp(ephemeral_pk, remote_ephemeral_pk, crypto_box_PUBLICKEYBYTES) > 0) ? &crypto_kx_server_session_keys : &crypto_kx_client_session_keys;

		if ((*kdf)(rx, tx, ephemeral_pk, ephemeral_sk, remote_ephemeral_pk) != 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Key derivation failure",
				src_addr.to_string(),
				dst_addr.to_string()
			);
			return;
		}

		randombytes_buf(nonce, crypto_aead_aes256gcm_NPUBBYTES);
		crypto_aead_aes256gcm_beforenm(&rx_ctx, rx);
		crypto_aead_aes256gcm_beforenm(&tx_ctx, tx);

		state_timer.stop();
		state_timer_interval = 0;

		this->dst_conn_id = packet.dst_conn_id();

		send_CONF();

		conn_state = ConnectionState::Established;

		if(dialled) {
			delegate->did_dial(*this);
		}

		break;
	}

	case ConnectionState::DialRcvd: {
		// Usually happend in case of simultaneous open
		auto src_conn_id = packet.src_conn_id();
		auto dst_conn_id = packet.dst_conn_id();
		if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) {
			// On conn id mismatch, send RST for that id
			// Connection should ideally be reestablished by
			// dial retry sending another DIAL
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Connection id mismatch: {}, {}, {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				src_conn_id,
				this->src_conn_id,
				dst_conn_id,
				this->dst_conn_id
			);
			send_RST(src_conn_id, dst_conn_id);
			return;
		}

		state_timer.stop();
		state_timer_interval = 0;

		send_CONF();

		conn_state = ConnectionState::Established;

		if(dialled) {
			delegate->did_dial(*this);
		}

		break;
	}

	case ConnectionState::Established: {
		auto src_conn_id = packet.src_conn_id();
		auto dst_conn_id = packet.dst_conn_id();
		if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Connection id mismatch: {}, {}, {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				src_conn_id,
				this->src_conn_id,
				dst_conn_id,
				this->dst_conn_id
			);
			send_RST(src_conn_id, dst_conn_id);
			return;
		}

		send_CONF();

		break;
	}

	case ConnectionState::Listen: {
		// Shouldn't receive DIALCONF in these states, unrecoverable
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: DIALCONF: Unexpected",
			src_addr.to_string(),
			dst_addr.to_string()
		);
		send_RST(packet.src_conn_id(), packet.dst_conn_id());

		break;
	}

	case ConnectionState::Closing: {
		// Ignore
		break;
	}
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_CONF() {
	transport.send(
		CONF()
		.set_src_conn_id(this->src_conn_id)
		.set_dst_conn_id(this->dst_conn_id)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_CONF(
	CONF &&packet
) {
	if(!packet.validate()) {
		return;
	}

	switch(conn_state) {
	case ConnectionState::DialRcvd: {
		auto src_conn_id = packet.src_conn_id();
		auto dst_conn_id = packet.dst_conn_id();
		if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: CONF: Connection id mismatch: {}, {}, {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				src_conn_id,
				this->src_conn_id,
				dst_conn_id,
				this->dst_conn_id
			);
			send_RST(src_conn_id, dst_conn_id);

			return;
		}

		conn_state = ConnectionState::Established;

		if(dialled) {
			delegate->did_dial(*this);
		}

		break;
	}

	case ConnectionState::Established: {
		auto src_conn_id = packet.src_conn_id();
		auto dst_conn_id = packet.dst_conn_id();
		if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: CONF: Connection id mismatch: {}, {}, {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				src_conn_id,
				this->src_conn_id,
				dst_conn_id,
				this->dst_conn_id
			);
			send_RST(src_conn_id, dst_conn_id);

			return;
		}

		break;
	}

	case ConnectionState::Listen:
	case ConnectionState::DialSent: {
		// Shouldn't receive CONF in these states, unrecoverable
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: CONF: Unexpected",
			src_addr.to_string(),
			dst_addr.to_string()
		);
		send_RST(packet.src_conn_id(), packet.dst_conn_id());

		break;
	}

	case ConnectionState::Closing: {
		// Ignore
		break;
	}
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_RST(
	uint32_t src_conn_id,
	uint32_t dst_conn_id
) {
	transport.send(
		RST()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_RST(
	RST &&packet
) {
	if(!packet.validate()) {
		return;
	}

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id == this->src_conn_id && dst_conn_id == this->dst_conn_id) {
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: RST",
			src_addr.to_string(),
			dst_addr.to_string()
		);
		reset();
		transport.close();
	} else if (conn_state == ConnectionState::Listen) {
		// Remove idle connection, usually happens if multiple RST are sent
		reset();
		transport.close();
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_DATA(
	SendStream &stream,
	DataItem &data_item,
	uint64_t offset,
	uint16_t length
) {
	this->last_sent_packet++;

	bool is_fin = (stream.done_queueing &&
		data_item.stream_offset + offset + length >= stream.queue_offset);

	auto packet = DATA(12 + length + crypto_aead_aes256gcm_ABYTES, is_fin)
					.set_src_conn_id(src_conn_id)
					.set_dst_conn_id(dst_conn_id)
					.set_packet_number(this->last_sent_packet)
					.set_stream_id(stream.stream_id)
					.set_offset(data_item.stream_offset + offset)
					.set_length(length)
					.payload_buffer();

	// Figure out better way
	packet.uncover_unsafe(30);
	packet.write_unsafe(30, data_item.data.data()+offset, length);
	packet.write_unsafe(30 + length + crypto_aead_aes256gcm_ABYTES, nonce, 12);

	if constexpr (is_encrypted) {
		crypto_aead_aes256gcm_encrypt_afternm(
			packet.data() + 18,
			nullptr,
			packet.data() + 18,
			12 + length,
			packet.data() + 2,
			16,
			nullptr,
			nonce,
			&tx_ctx
		);
		sodium_increment(nonce, 12);
	}

	this->sent_packets.emplace(
		std::piecewise_construct,
		std::forward_as_tuple(this->last_sent_packet),
		std::forward_as_tuple(
			asyncio::EventLoop::now(),
			&stream,
			&data_item,
			offset,
			length
		)
	);

	transport.send(std::move(packet));

	if(is_fin && stream.state != SendStream::State::Acked) {
		stream.state = SendStream::State::Sent;
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_DATA(
	DATA &&packet
) {
	if(!packet.validate(12 + crypto_aead_aes256gcm_ABYTES)) {
		return;
	}

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id, send RST
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: DATA: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		send_RST(src_conn_id, dst_conn_id);
		return;
	}

	if constexpr (is_encrypted) {
		auto res = crypto_aead_aes256gcm_decrypt_afternm(
			packet.payload() - 12,
			nullptr,
			nullptr,
			packet.payload() - 12,
			packet.payload_buffer().size(),
			packet.payload() - 28,
			16,
			packet.payload() + packet.payload_buffer().size() - 12,
			&rx_ctx
		);

		if(res < 0) {
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: DATA: Decryption failure: {}, {}",
				src_addr.to_string(),
				dst_addr.to_string(),
				this->src_conn_id,
				this->dst_conn_id
			);
			send_RST(src_conn_id, dst_conn_id);
			return;
		}
	}

	SPDLOG_TRACE("DATA <<< {}: {}, {}", dst_addr.to_string(), packet.offset(), packet.length());

	if(conn_state == ConnectionState::DialRcvd) {
		conn_state = ConnectionState::Established;

		if(dialled) {
			delegate->did_dial(*this);
		}
	} else if(conn_state != ConnectionState::Established) {
		return;
	}

	auto offset = packet.offset();
	auto length = packet.length();
	auto packet_number = packet.packet_number();

	auto &stream = get_or_create_recv_stream(packet.stream_id());

	// Short circuit once stream has been received fully.
	if(stream.state == RecvStream::State::AllRecv ||
		stream.state == RecvStream::State::Read) {
		return;
	}

	// Short circuit if stream is waiting to be flushed.
	if(stream.wait_flush) {
		return;
	}

	// Set stream size if fin bit set
	if(packet.is_fin_set() && stream.state == RecvStream::State::Recv) {
		stream.size = offset + length;
		stream.state = RecvStream::State::SizeKnown;
	}

	auto p = std::move(packet).payload_buffer();
	p.truncate_unsafe(crypto_aead_aes256gcm_ABYTES + 12);

	// Check if length matches packet
	// FIXME: Why even have length? Can just set from the packet
	if(p.size() != length) {
		return;
	}

	// Add to ack range
	ack_ranges.add_packet_number(packet_number);

	// Start ack delay timer if not already active
	if(!ack_timer_active) {
		ack_timer_active = true;
		ack_timer.template start<Self, &Self::ack_timer_cb>(25, 0);
	}

	// Short circuit on no new data
	if(offset + length <= stream.read_offset) {
		return;
	}

	// Check if data can be read immediately
	if(offset <= stream.read_offset) {
		// Cover bytes which have already been read
		p.cover_unsafe(stream.read_offset - offset);

		// Read bytes and update offset
		auto res = delegate->did_recv(*this, std::move(p), stream.stream_id);
		if(res < 0) {
			return;
		}
		stream.read_offset = offset + length;

		// Read any out of order data
		auto iter = stream.recv_packets.begin();
		while(iter != stream.recv_packets.end()) {
			// Short circuit if data can't be read immediately
			if(iter->second.offset > stream.read_offset) {
				break;
			}

			auto offset = iter->second.offset;
			auto length = iter->second.length;

			// Check new data
			if(offset + length > stream.read_offset) {
				// Cover bytes which have already been read
				iter->second.packet.cover_unsafe(stream.read_offset - offset);

				// Read bytes and update offset
				SPDLOG_DEBUG("Out of order: {}, {}, {:spn}", offset, length, spdlog::to_hex(iter->second.packet.data(), iter->second.packet.data() + iter->second.packet.size()));
				auto res = delegate->did_recv(*this, std::move(iter->second).packet, stream.stream_id);
				if(res < 0) {
					return;
				}

				stream.read_offset = offset + length;
			}

			// Next iter
			iter = stream.recv_packets.erase(iter);
		}

		// Check all data read
		if(stream.check_read()) {
			stream.state = RecvStream::State::Read;
			recv_streams.erase(stream.stream_id);
		}
	} else {
		// Queue packet for later processing
		SPDLOG_DEBUG("Queue for later: {}, {}, {:spn}", offset, length, spdlog::to_hex(p.data(), p.data() + p.size()));
		stream.recv_packets.try_emplace(
			offset,
			asyncio::EventLoop::now(),
			offset,
			length,
			std::move(p)
		);

		// Check all data received
		if (stream.check_finish()) {
			stream.state = RecvStream::State::AllRecv;
		}
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_ACK() {
	size_t size = ack_ranges.ranges.size() > 171 ? 171 : ack_ranges.ranges.size();

	transport.send(
		ACK(size)
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
		.set_packet_number(ack_ranges.largest)
		.set_size(size)
		.set_ranges(ack_ranges.ranges.begin(), ack_ranges.ranges.end())
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_ACK(
	ACK &&packet
) {
	if(!packet.validate()) {
		return;
	}

	if(conn_state != ConnectionState::Established) {
		return;
	}

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id, send RST
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: ACK: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		send_RST(src_conn_id, dst_conn_id);
		return;
	}

	auto now = asyncio::EventLoop::now();

	uint64_t largest = packet.packet_number();

	// New largest acked packet
	if(largest > largest_acked && sent_packets.find(largest) != sent_packets.end()) {
		auto &sent_packet = sent_packets[largest];

		// Update largest packet details
		largest_acked = largest;
		largest_sent_time = sent_packet.sent_time;

		// Update RTT estimate
		if(rtt < 0) {
			rtt = now - sent_packet.sent_time;
		} else {
			rtt = 0.875 * rtt + 0.125 * (now - sent_packet.sent_time);
		}
	}

	uint64_t high = largest;
	bool gap = false;
	bool is_app_limited = (bytes_in_flight < 0.8 * congestion_window);

	for(
		auto iter = packet.ranges_begin();
		iter != packet.ranges_end();
		++iter, gap = !gap
	) {
		uint64_t range = *iter;

		uint64_t low = high - range;

		// Short circuit on gap range
		if(gap) {
			high = low;
			continue;
		}

		//TODO: if either of high, low+1 are negative throw encoding error
		//  https://tools.ietf.org/html/draft-ietf-quic-transport-34#section-19.3.1
		//Tips	  https://wesmckinney.com/blog/avoid-unsigned-integers/
		if ( low+1 > high){
			break;
		}

		// Get packets within range [low+1, high]
		auto low_iter = sent_packets.lower_bound(low + 1);
		auto high_iter = sent_packets.upper_bound(high);

		// Iterate acked packets
		for(
			;
			low_iter != high_iter;
			low_iter = sent_packets.erase(low_iter)
		) {
			auto &sent_packet = low_iter->second;
			auto &stream = *sent_packet.stream;

			auto sent_offset = sent_packet.data_item->stream_offset + sent_packet.offset;

			if(stream.acked_offset < sent_offset) {
				// Out of order ack, store for later processing
				stream.outstanding_acks[sent_offset] = sent_packet.length;
			} else if(stream.acked_offset < sent_offset + sent_packet.length) {
				// In order ack
				stream.acked_offset = sent_offset + sent_packet.length;

				// Process outstanding acks if possible
				for(
					auto iter = stream.outstanding_acks.begin();
					iter != stream.outstanding_acks.end();
					iter = stream.outstanding_acks.erase(iter)
				) {
					if(iter->first > stream.acked_offset) {
						// Out of order, abort
						break;
					}

					uint64_t new_acked_offset = iter->first + iter->second;
					if(stream.acked_offset < new_acked_offset) {
						stream.acked_offset = new_acked_offset;
					}
				}

				bool fully_acked = true;
				// Cleanup acked data items
				for(
					auto iter = stream.data_queue.begin();
					iter != stream.data_queue.end();
					iter = stream.data_queue.erase(iter)
				) {
					if(stream.acked_offset < iter->stream_offset + iter->data.size()) {
						// Still not fully acked, skip erase and abort
						fully_acked = false;
						break;
					}
					// TODO: Handle delegate->did_send return type for connection continuation
					delegate->did_send(
						*this,
						std::move(iter->data)
					);

				}

				if(fully_acked) {
					stream.next_item_iterator = stream.data_queue.end();
				}
			} else {
				// Already acked range, ignore
			}

			// Cleanup
			stream.bytes_in_flight -= sent_packet.length;
			bytes_in_flight -= sent_packet.length;

			SPDLOG_TRACE("Times: {}, {}", sent_packet.sent_time, congestion_start);

			// Congestion control
			// Check if not in congestion recovery and not application limited
			if(sent_packet.sent_time > congestion_start && !is_app_limited) {
				if(congestion_window < ssthresh) {
					// Slow start, exponential increase
					congestion_window += sent_packet.length;
				} else {
					// Congestion avoidance, CUBIC
					// auto k = k;
					// auto t = now - congestion_start;
					// congestion_window = w_max + 4 * std::pow(0.001 * (t - k), 3);

					// if(congestion_window < 10000) {
					// 	congestion_window = 10000;
					// }

					// Congestion avoidance, NEW RENO
					congestion_window += 1500 * sent_packet.length / congestion_window;
				}
			}

			// Check stream finish
			if (stream.state == SendStream::State::Sent &&
				stream.acked_offset == stream.queue_offset) {
				stream.state = SendStream::State::Acked;

				// TODO: Call delegate to inform
				SPDLOG_DEBUG("Acked: {}", stream.stream_id);

				// Remove stream
				send_streams.erase(stream.stream_id);

				return;
			}

		}

		high = low;
	}

	auto sent_iter = sent_packets.begin();

	// Determine lost packets
	while(sent_iter != sent_packets.end()) {
		// Condition for packet in flight to be considered lost
		// 1. more than 20 packets before largest acked - disabled for now
		// 2. more than 25ms before before largest acked
		if (/*sent_iter->first + 20 < largest_acked ||*/
			largest_sent_time > sent_iter->second.sent_time + 50) {
			SPDLOG_TRACE(
				"Stream transport {{ Src: {}, Dst: {} }}: Lost packet: {}, {}, {}",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				sent_iter->first,
				largest_sent_time,
				sent_iter->second.sent_time
			);

			bytes_in_flight -= sent_iter->second.length;
			sent_iter->second.stream->bytes_in_flight -= sent_iter->second.length;
			lost_packets[sent_iter->first] = sent_iter->second;

			sent_iter++;
		} else {
			break;
		}
	}

	if(sent_iter == sent_packets.begin()) {
		// No lost packets, ignore
	} else {
		// Lost packets, congestion event
		auto last_iter = std::prev(sent_iter);
		auto &sent_packet = last_iter->second;

		if(sent_packet.sent_time > congestion_start) {
			// New congestion event
			SPDLOG_DEBUG(
				"Stream transport {{ Src: {}, Dst: {} }}: Congestion event: {}, {}",
				transport.src_addr.to_string(),
				transport.dst_addr.to_string(),
				congestion_window,
				last_iter->first
			);
			congestion_start = now;

			if(congestion_window < w_max) {
				// Fast convergence
				w_max = congestion_window;
				congestion_window *= 0.6;
			} else {
				w_max = congestion_window;
				congestion_window *= 0.75;
			}

			if(congestion_window < 10000) {
				congestion_window = 10000;
			}
			ssthresh = congestion_window;
			k = std::cbrt(w_max / 16)*1000;
		}

		// Pop lost packets from sent
		sent_packets.erase(sent_packets.begin(), sent_iter);
	}

	// New packets
	send_pending_data();

	tlp_interval = DEFAULT_TLP_INTERVAL;
	tlp_timer.template start<Self, &Self::tlp_timer_cb>(tlp_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_SKIPSTREAM(
	uint16_t stream_id,
	uint64_t offset
) {
	transport.send(
		SKIPSTREAM()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
		.set_stream_id(stream_id)
		.set_offset(offset)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_SKIPSTREAM(
	SKIPSTREAM &&packet
) {
	if(!packet.validate()) {
		return;
	}

	SPDLOG_TRACE("SKIPSTREAM <<< {}: {}, {}", dst_addr.to_string(), packet.stream_id(), packet.offset());

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id, send RST
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: SKIPSTREAM: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		send_RST(src_conn_id, dst_conn_id);
		return;
	}

	if(conn_state != ConnectionState::Established) {
		return;
	}

	auto stream_id = packet.stream_id();
	auto offset = packet.offset();
	auto &stream = get_or_create_send_stream(stream_id);

	// Check for duplicate
	if(offset < stream.acked_offset) {
		send_FLUSHSTREAM(stream_id, stream.acked_offset);
		return;
	}

	delegate->did_recv_skip_stream(*this, stream_id);

	flush_stream(stream_id);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_FLUSHSTREAM(
	uint16_t stream_id,
	uint64_t offset
) {
	transport.send(
		FLUSHSTREAM()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
		.set_stream_id(stream_id)
		.set_offset(offset)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_FLUSHSTREAM(
	FLUSHSTREAM &&packet
) {
	if(!packet.validate()) {
		return;
	}

	SPDLOG_TRACE("FLUSHSTREAM <<< {}: {}, {}", dst_addr.to_string(), packet.stream_id(), packet.offset());

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id, send RST
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: FLUSHSTREAM: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		send_RST(src_conn_id, dst_conn_id);
		return;
	}

	if(conn_state != ConnectionState::Established) {
		return;
	}

	auto stream_id = packet.stream_id();
	auto offset = packet.offset();

	auto &stream = get_or_create_recv_stream(stream_id);
	auto old_offset = stream.read_offset;

	// Check for duplicate
	if(old_offset >= offset) {
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: Duplicate flush: {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			stream_id
		);
		return;
	}

	stream.state_timer.stop();

	stream.read_offset = offset;
	stream.wait_flush = false;

	delegate->did_recv_flush_stream(*this, stream_id, offset, old_offset);

	send_FLUSHCONF(stream_id);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_FLUSHCONF(
	uint16_t stream_id
) {
	transport.send(
		FLUSHCONF()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
		.set_stream_id(stream_id)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_FLUSHCONF(
	FLUSHCONF &&packet
) {
	if(!packet.validate()) {
		return;
	}

	SPDLOG_TRACE("FLUSHCONF <<< {}: {}", dst_addr.to_string(), packet.stream_id());

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id, send RST
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: FLUSHCONF: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		send_RST(src_conn_id, dst_conn_id);
		return;
	}

	if(conn_state != ConnectionState::Established) {
		return;
	}

	auto stream_id = packet.stream_id();
	auto &stream = get_or_create_send_stream(stream_id);

	stream.state_timer.stop();

	delegate->did_recv_flush_conf(*this, stream_id);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_CLOSE(uint16_t reason) {
	transport.send(
		CLOSE()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
		.set_reason(reason)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_CLOSE(
	CLOSE &&packet
) {
	if(!packet.validate()) {
		return;
	}

	SPDLOG_TRACE("CLOSE <<< {}: {}", dst_addr.to_string(), packet.stream_id());

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: CLOSE: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		// If connection ids don't match, send CLOSECONF so the other side can close if needed
		// but don't reset/close the current connection in case the CLOSE is stale
		send_CLOSECONF(src_conn_id, dst_conn_id);
		if(conn_state == ConnectionState::Listen) {
			// Close idle connections
			reset();
			transport.close();
		}
		return;
	}

	// Transport is closed after sending CLOSECONF
	// A new transport will be created in case the other side retries and will follow the above code path
	if(conn_state == ConnectionState::Established || conn_state == ConnectionState::Closing) {
		send_CLOSECONF(src_conn_id, dst_conn_id);
		reset();
		transport.close(packet.reason());
	} else if(conn_state == ConnectionState::Listen) {
		// Close idle connections
		reset();
		transport.close();
	} else {
		// Ignore in other states
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::send_CLOSECONF(
	uint32_t src_conn_id,
	uint32_t dst_conn_id
) {
	transport.send(
		CLOSECONF()
		.set_src_conn_id(src_conn_id)
		.set_dst_conn_id(dst_conn_id)
	);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv_CLOSECONF(
	CLOSECONF &&packet
) {
	if(!packet.validate()) {
		return;
	}

	SPDLOG_TRACE("CLOSECONF <<< {}: {}", dst_addr.to_string(), packet.stream_id());

	state_timer.stop();

	auto src_conn_id = packet.src_conn_id();
	auto dst_conn_id = packet.dst_conn_id();
	if(src_conn_id != this->src_conn_id || dst_conn_id != this->dst_conn_id) { // Wrong connection id
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: CLOSECONF: Connection id mismatch: {}, {}, {}, {}",
			src_addr.to_string(),
			dst_addr.to_string(),
			src_conn_id,
			this->src_conn_id,
			dst_conn_id,
			this->dst_conn_id
		);
		// Not meant for this connection, stale CLOSECONF
		return;
	}

	reset();
	transport.close();
}

//---------------- Protocol functions end ----------------//


//---------------- Delegate functions begin ----------------//

//! Callback function when trying to establish a connection with a peer. Sends a DIAL packet to initiate the handshake
template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_dial(
	BaseTransport &,
	uint8_t const* remote_static_pk
) {
	if(conn_state != ConnectionState::Listen) {
		return;
	}

	if (remote_static_pk != nullptr) {
		std::memcpy(this->remote_static_pk, remote_static_pk, crypto_box_PUBLICKEYBYTES);
	}

	// Begin handshake
	dialled = true;

	state_timer_interval = 1000;
	state_timer.template start<Self, &Self::dial_timer_cb>(state_timer_interval, 0);

	src_conn_id = (uint32_t)std::random_device()();
	send_DIAL();
	conn_state = ConnectionState::DialSent;
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_close(
	BaseTransport &,
	uint16_t reason
) {
	delegate->did_close(*this, reason);
	transport_manager.erase(dst_addr);
}

//! Receives the packet and processes them
/*!
	Determines the type of packet by reading the first byte and redirects the packet to appropriate function for further processing

	\li \b byte :	\b type
	\li 0		:	DATA
	\li 1		:	DATA + FIN
	\li 2		:	ACK
	\li 3		:	DIAL
	\li 4		:	DIALCONF
	\li 5		:	CONF
	\li 6		:	RST
*/
template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_recv(
	BaseTransport &,
	BaseMessageType &&packet
) {
	auto type = packet.payload_buffer().read_uint8(1);
	if(type == std::nullopt || packet.payload_buffer().read_uint8_unsafe(0) != 0) {
		return;
	}

	switch(type.value()) {
		// DATA
		case 0:
		// DATA + FIN
		case 1: did_recv_DATA(std::move(packet));
		break;
		// ACK
		case 2: did_recv_ACK(std::move(packet));
		break;
		// DIAL
		case 3: did_recv_DIAL(std::move(packet));
		break;
		// DIALCONF
		case 4: did_recv_DIALCONF(std::move(packet));
		break;
		// CONF
		case 5: did_recv_CONF(std::move(packet));
		break;
		// RST
		case 6: did_recv_RST(std::move(packet));
		break;
		// SKIPSTREAM
		case 7: did_recv_SKIPSTREAM(std::move(packet));
		break;
		// FLUSHSTREAM
		case 8: did_recv_FLUSHSTREAM(std::move(packet));
		break;
		// FLUSHCONF
		case 9: did_recv_FLUSHCONF(std::move(packet));
		break;
		case 12: did_recv_CLOSE(std::move(packet));
		break;
		case 11: did_recv_CLOSECONF(std::move(packet));
		break;		// UNKNOWN
		default: SPDLOG_TRACE("UNKNOWN <<< {}", dst_addr.to_string());
		break;
	}
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::did_send(
	BaseTransport &,
	core::Buffer &&packet
) {
	auto type = packet.read_uint8(1);
	if(type == std::nullopt) {
		return;
	}

	switch(type.value()) {
		// DATA
		case 0: SPDLOG_TRACE("DATA >>> {}", dst_addr.to_string());
		break;
		// DATA + FIN
		case 1: SPDLOG_TRACE("DATA + FIN >>> {}", dst_addr.to_string());
		break;
		// ACK
		case 2: SPDLOG_TRACE("ACK >>> {}", dst_addr.to_string());
		break;
		// DIAL
		case 3: SPDLOG_TRACE("DIAL >>> {}", dst_addr.to_string());
		break;
		// DIALCONF
		case 4: SPDLOG_TRACE("DIALCONF >>> {}", dst_addr.to_string());
		break;
		// CONF
		case 5: SPDLOG_TRACE("CONF >>> {}", dst_addr.to_string());
		break;
		// RST
		case 6: SPDLOG_TRACE("RST >>> {}", dst_addr.to_string());
		break;
		// SKIPSTREAM
		case 7: SPDLOG_TRACE("SKIPSTREAM >>> {}", dst_addr.to_string());
		break;
		// FLUSHSTREAM
		case 8: SPDLOG_TRACE("FLUSHSTREAM >>> {}", dst_addr.to_string());
		break;
		// FLUSHCONF
		case 9: SPDLOG_TRACE("FLUSHCONF >>> {}", dst_addr.to_string());
		break;
		// UNKNOWN
		default: SPDLOG_TRACE("UNKNOWN >>> {}", dst_addr.to_string());
		break;
	}
}

//---------------- Delegate functions end ----------------//

template<typename DelegateType, template<typename> class DatagramTransport>
StreamTransport<DelegateType, DatagramTransport>::StreamTransport(
	core::SocketAddress const &src_addr,
	core::SocketAddress const &dst_addr,
	BaseTransport &transport,
	core::TransportManager<StreamTransport<DelegateType, DatagramTransport>> &transport_manager
) : transport(transport),
	transport_manager(transport_manager),
	state_timer(this),
	pacing_timer(this),
	tlp_timer(this),
	ack_timer(this),
	src_addr(src_addr),
	dst_addr(dst_addr),
	delegate(nullptr) {
	if(sodium_init() == -1) {
		throw;
	}
}


template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::setup(
	DelegateType *delegate,
	uint8_t const* static_sk
) {
	this->delegate = delegate;

	std::memcpy(this->static_sk, static_sk, crypto_box_SECRETKEYBYTES);
	crypto_scalarmult_base(this->static_pk, this->static_sk);

	crypto_kx_keypair(this->ephemeral_pk, this->ephemeral_sk);

	transport.setup(this);
}


template<typename DelegateType, template<typename> class DatagramTransport>
int StreamTransport<DelegateType, DatagramTransport>::send(
	core::Buffer &&bytes,
	uint16_t stream_id
) {
	if (conn_state != ConnectionState::Established) {
		return -2;
	}
	auto &stream = get_or_create_send_stream(stream_id);

	if(stream.state == SendStream::State::Ready) {
		stream.state = SendStream::State::Send;
	}

	// Abort if data queue is too big
	if(stream.queue_offset - stream.acked_offset > 20000000) {
		SPDLOG_DEBUG("Data queue overflow");
		return -1;
	}

	auto size = bytes.size();

	// Check idle stream
	bool idle = stream.next_item_iterator == stream.data_queue.end();

	// Add data to send queue
	stream.data_queue.emplace_back(
		std::move(bytes),
		stream.queue_offset
	);

	stream.queue_offset += size;

	// Handle idle stream
	if(idle) {
		stream.next_item_iterator = std::prev(stream.data_queue.end());
	}

	// Handle idle connection
	if(sent_packets.size() == 0 && lost_packets.size() == 0 && send_queue.size() == 0) {
		tlp_timer.template start<Self, &Self::tlp_timer_cb>(tlp_interval, 0);
	}

	register_send_intent(stream);
	send_pending_data();

	return 0;
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::close(uint16_t reason) {
	if( !is_active() )
		return;

	// Preserve conn ids so retries work
	auto src_conn_id = this->src_conn_id;
	auto dst_conn_id = this->dst_conn_id;
	reset();
	this->src_conn_id = src_conn_id;
	this->dst_conn_id = dst_conn_id;

	// Initiate close
	conn_state = ConnectionState::Closing;
	close_reason = reason;
	send_CLOSE(reason);

	state_timer_interval = 1000;
	state_timer.template start<Self, &Self::close_timer_cb>(state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::close_timer_cb() {
	if ( !is_active() )
		return;

	if(state_timer_interval >= 8000) { // Abort on too many retries
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: Close timeout",
			this->src_addr.to_string(),
			this->dst_addr.to_string()
		);
		reset();
		transport.close();
		return;
	}

	send_CLOSE(close_reason);

	state_timer_interval *= 2;
	state_timer.template start<Self, &Self::close_timer_cb>(state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
bool StreamTransport<DelegateType, DatagramTransport>::is_active() {
	if(conn_state == ConnectionState::Established) {
		return true;
	}

	return false;
}

template<typename DelegateType, template<typename> class DatagramTransport>
double StreamTransport<DelegateType, DatagramTransport>::get_rtt() {
	return rtt;
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::skip_timer_cb(RecvStream& stream) {
	if ( !is_active() )
		return;
	if(stream.state_timer_interval >= 64000) { // Abort on too many retries
		stream.state_timer_interval = 0;
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: Skip timeout: {}",
			this->src_addr.to_string(),
			this->dst_addr.to_string(),
			stream.stream_id
		);
		reset();
		transport.close();
		return;
	}

	uint64_t offset;

	if(stream.recv_packets.size() > 0) {
		auto &packet = stream.recv_packets.rbegin()->second;
		offset = packet.offset + packet.length;
	} else {
		offset = stream.read_offset;
	}

	this->send_SKIPSTREAM(stream.stream_id, offset);

	stream.state_timer_interval *= 2;
	stream.state_timer.template start<Self, RecvStream, &Self::skip_timer_cb>(stream.state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::skip_stream(
	uint16_t stream_id
) {
	auto &stream = get_or_create_recv_stream(stream_id);
	stream.wait_flush = true;

	uint64_t offset;

	if(stream.recv_packets.size() > 0) {
		auto &packet = stream.recv_packets.rbegin()->second;
		offset = packet.offset + packet.length;
	} else {
		offset = stream.read_offset;
	}

	send_SKIPSTREAM(stream_id, offset);

	stream.state_timer_interval = 1000;
	stream.state_timer.template start<Self, RecvStream, &Self::skip_timer_cb>(stream.state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::flush_timer_cb(SendStream& stream) {
	if ( !is_active() )
		return;	
	if(stream.state_timer_interval >= 64000) { // Abort on too many retries
		stream.state_timer_interval = 0;
		SPDLOG_DEBUG(
			"Stream transport {{ Src: {}, Dst: {} }}: Flush timeout: {}",
			this->src_addr.to_string(),
			this->dst_addr.to_string(),
			stream.stream_id
		);
		reset();
		transport.close();
		return;
	}

	this->send_FLUSHSTREAM(stream.stream_id, stream.sent_offset);

	stream.state_timer_interval *= 2;
	stream.state_timer.template start<Self, SendStream, &Self::flush_timer_cb>(stream.state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
void StreamTransport<DelegateType, DatagramTransport>::flush_stream(
	uint16_t stream_id
) {
	auto &stream = get_or_create_send_stream(stream_id);

	// Remove previously sent packets
	auto sent_iter = sent_packets.cbegin();
	while(sent_iter != sent_packets.cend()) {
		if(sent_iter->second.stream->stream_id != stream.stream_id) {
			sent_iter++;
			continue;
		}

		bytes_in_flight -= sent_iter->second.length;
		sent_iter = sent_packets.erase(sent_iter);
	}

	// Remove lost packets
	auto lost_iter = lost_packets.cbegin();
	while(lost_iter != lost_packets.cend()) {
		if(lost_iter->second.stream->stream_id != stream.stream_id) {
			lost_iter++;
			continue;
		}

		bytes_in_flight -= lost_iter->second.length;
		lost_iter = lost_packets.erase(lost_iter);
	}

	stream.data_queue.clear();
	stream.queue_offset = stream.sent_offset;
	stream.next_item_iterator = stream.data_queue.end();
	stream.bytes_in_flight = 0;
	stream.acked_offset = stream.sent_offset;
	stream.outstanding_acks.clear();

	send_FLUSHSTREAM(stream_id, stream.sent_offset);

	stream.state_timer_interval = 1000;
	stream.state_timer.template start<Self, SendStream, &Self::flush_timer_cb>(stream.state_timer_interval, 0);
}

template<typename DelegateType, template<typename> class DatagramTransport>
bool StreamTransport<DelegateType, DatagramTransport>::is_internal() {
	return transport.is_internal();
}

template<typename DelegateType, template<typename> class DatagramTransport>
uint8_t const* StreamTransport<DelegateType, DatagramTransport>::get_static_pk() {
	return static_pk;
}

template<typename DelegateType, template<typename> class DatagramTransport>
uint8_t const* StreamTransport<DelegateType, DatagramTransport>::get_remote_static_pk() {
	return remote_static_pk;
}

} // namespace stream
} // namespace marlin

#endif // MARLIN_STREAM_STREAMTRANSPORT_HPP
