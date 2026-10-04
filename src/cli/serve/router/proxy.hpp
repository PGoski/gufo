#ifndef GUFO_CLI_SERVE_ROUTER_PROXY_HPP_
#define GUFO_CLI_SERVE_ROUTER_PROXY_HPP_

#include <string>

#include "src/cli/serve/http_server.hpp"

namespace gufo::router {

struct UpstreamTarget {
  std::string host{"127.0.0.1"};
  int port{0};
};

/// Forward one buffered front request to the worker and produce a front
/// response. The returned response streams the worker body through
/// `HttpResponse::streaming_body`, checking `request.is_cancelled` between
/// chunks. Connect failure / EOF before headers → 502 `upstream_unavailable`
/// JSON error. Hop-by-hop headers (Connection, Keep-Alive, Transfer-Encoding,
/// Upgrade) are stripped both ways; `Authorization`, `Host`, `Connection`
/// are not forwarded.
gufo::server::HttpResponse ProxyToWorker(
    const UpstreamTarget& target, const gufo::server::HttpRequest& request);

}  // namespace gufo::router

#endif  // GUFO_CLI_SERVE_ROUTER_PROXY_HPP_
