#ifndef GUFO_CLI_SERVE_ROUTER_WS_RELAY_HPP_
#define GUFO_CLI_SERVE_ROUTER_WS_RELAY_HPP_

#include <string>

#include "src/cli/serve/router/proxy.hpp"
#include "src/cli/serve/websocket.hpp"

namespace gufo::router {

/// Perform the RFC 6455 client handshake against the worker (fresh
/// Sec-WebSocket-Key, require 101 + correct Sec-WebSocket-Accept computed via
/// SHA-1), then pump raw bytes in both directions until either side closes or
/// `front.cancelled()`. Frames are never parsed or re-masked: client frames
/// are already masked and the worker's replies are not, so both directions
/// pass through untouched. The front frame reader is stopped for the duration
/// and the descriptor stays owned by the front server. Any handshake failure
/// returns false without throwing.
bool RelayWebSocket(const UpstreamTarget& target,
                    gufo::server::WebSocket& front, std::string* error);

}  // namespace gufo::router

#endif  // GUFO_CLI_SERVE_ROUTER_WS_RELAY_HPP_
