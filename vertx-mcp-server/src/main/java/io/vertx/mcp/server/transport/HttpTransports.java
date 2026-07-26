package io.vertx.mcp.server.transport;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mcp.McpTransportContext;
import io.vertx.mcp.spec.JsonRpcCodec;

/**
 * Shared HTTP plumbing for the three HTTP server transports: header names, the checks the
 * MCP spec mandates on every request (Accept negotiation, {@code Origin} validation for
 * DNS-rebinding protection, {@code MCP-Protocol-Version} validation) and the JSON-RPC
 * error bodies that go with them.
 *
 * <p>
 * Kept in one place so the streamable, legacy-SSE and stateless transports cannot drift
 * apart on the wire semantics a client is entitled to rely on.
 */
public final class HttpTransports {

  /** Media type of a plain JSON reply. */
  public static final String APPLICATION_JSON = "application/json";

  /** Media type of an SSE stream. */
  public static final String TEXT_EVENT_STREAM = "text/event-stream";

  /** Key under which the default context extractor stashes the request headers. */
  public static final String HEADERS_KEY = "headers";

  /** Key under which the default context extractor stashes the {@link RoutingContext}. */
  public static final String ROUTING_CONTEXT_KEY = "routingContext";

  /**
   * Protocol revisions this SDK speaks. A client that pins {@code MCP-Protocol-Version}
   * to anything else is rejected rather than silently served a dialect it cannot parse.
   */
  public static final List<String> SUPPORTED_PROTOCOL_VERSIONS = List.of(ProtocolVersions.MCP_2024_11_05,
      ProtocolVersions.MCP_2025_03_26, ProtocolVersions.MCP_2025_06_18, ProtocolVersions.MCP_2025_11_25);

  /** Not in {@code McpSchema.ErrorCodes}: the transport-level "no such session" code. */
  public static final int SESSION_NOT_FOUND = -32001;

  private HttpTransports() {
  }

  /**
   * The default extractor: every request header plus the {@link RoutingContext} itself,
   * so a tool handler can reach an authenticated principal, a tenant id or the raw
   * request without the transport needing to know about them.
   */
  public static Function<RoutingContext, McpTransportContext> defaultContextExtractor() {
    return rc -> {
      Map<String, String> headers = new LinkedHashMap<>();
      rc.request().headers().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
      return McpTransportContext.of(Map.of(HEADERS_KEY, headers, ROUTING_CONTEXT_KEY, rc));
    };
  }

  /**
   * Whether the client said it can handle {@code mediaType}. A wildcard
   * {@code &#42;/&#42;} counts: it genuinely does include the type, and rejecting it
   * would break perfectly conformant HTTP clients for no protocol benefit.
   */
  public static boolean accepts(RoutingContext rc, String mediaType) {
    String accept = rc.request().getHeader("Accept");
    return accept != null && (accept.contains(mediaType) || accept.contains("*/*"));
  }

  /**
   * DNS-rebinding protection required by the spec for locally bound servers: a browser
   * page on another origin can reach {@code 127.0.0.1} but cannot forge {@code Origin}.
   * An absent header means a non-browser client, which this check is not aimed at.
   */
  public static boolean originAllowed(RoutingContext rc, Set<String> allowedOrigins) {
    if (allowedOrigins.isEmpty()) {
      return true;
    }
    String origin = rc.request().getHeader("Origin");
    return origin == null || allowedOrigins.contains(origin);
  }

  /** Absent means "the client predates the header", which the spec allows. */
  public static boolean protocolVersionSupported(RoutingContext rc) {
    String version = rc.request().getHeader(io.modelcontextprotocol.spec.HttpHeaders.PROTOCOL_VERSION);
    return version == null || SUPPORTED_PROTOCOL_VERSIONS.contains(version);
  }

  /**
   * Registers {@code handler} both at {@code path} and at {@code "/"}.
   *
   * <p>
   * A {@link Router} returned by these transports is meant to be mounted as a sub-router
   * ({@code router.route("/mcp*").subRouter(transport.router())}), and vertx-web strips
   * the mount prefix before the sub-router sees the request -- so the endpoint arrives as
   * {@code "/"}. Registering both shapes means the same router also works when it is
   * installed as the main router of an {@code HttpServer}.
   */
  static void routeBoth(Router router, io.vertx.core.http.HttpMethod method, String path,
      io.vertx.core.Handler<RoutingContext> handler) {
    router.route(method, path).handler(handler);
    if (!"/".equals(path)) {
      router.route(method, "/").handler(handler);
    }
  }

  /** Writes a JSON-RPC error response with the given HTTP status and ends the response. */
  public static void writeError(RoutingContext rc, JsonRpcCodec codec, int status, Object requestId, int code,
      String message) {
    McpSchema.JSONRPCResponse body = new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, requestId, null,
        new McpSchema.JSONRPCResponse.JSONRPCError(code, message, null));
    HttpServerResponse response = rc.response();
    if (response.ended()) {
      return;
    }
    response.setStatusCode(status).putHeader("Content-Type", APPLICATION_JSON).end(codec.encodeToBuffer(body));
  }

  /** Prepares a response for streaming SSE: no buffering anywhere along the path. */
  public static void openSseResponse(HttpServerResponse response) {
    response.setStatusCode(200)
      .setChunked(true)
      .putHeader("Content-Type", TEXT_EVENT_STREAM)
      .putHeader("Cache-Control", "no-cache")
      .putHeader("Connection", "keep-alive")
      // Nginx and friends buffer unknown content by default, which stalls an SSE stream
      // until the buffer fills. This is the conventional opt-out.
      .putHeader("X-Accel-Buffering", "no");
  }
}
