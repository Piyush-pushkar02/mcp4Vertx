package io.vertx.mcp.client.transport;

/**
 * The two {@code Content-Type} tests every MCP HTTP response is dispatched on.
 *
 * <p>
 * Both are prefix matches on the lower-cased header because servers routinely append
 * parameters -- {@code text/event-stream;charset=UTF-8} is the same media type as
 * {@code text/event-stream}, and refusing it would break interoperability for no reason.
 */
final class MediaTypes {

  private MediaTypes() {
  }

  /** Whether the body is a Server-Sent Events stream and must be parsed incrementally. */
  static boolean isEventStream(String contentType) {
    return contentType != null && contentType.toLowerCase().startsWith("text/event-stream");
  }

  /** Whether the body is a single JSON-RPC message that can simply be collected. */
  static boolean isJson(String contentType) {
    return contentType != null && contentType.toLowerCase().startsWith("application/json");
  }
}
