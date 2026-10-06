#ifndef GUFO_CLI_SERVE_ROUTER_ROUTER_HPP_
#define GUFO_CLI_SERVE_ROUTER_ROUTER_HPP_

#include <span>

namespace gufo::cli {

/// `gufo router`: validate a preset file, optionally preload workers, and
/// front the preset models on one listener with request relaying, held
/// loads, admission, websocket relay and worker supervision.
int RunRouter(std::span<const char* const> args);

}  // namespace gufo::cli

#endif  // GUFO_CLI_SERVE_ROUTER_ROUTER_HPP_
