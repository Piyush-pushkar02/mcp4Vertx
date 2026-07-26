package io.vertx.mcp.client.transport;

/**
 * A transport-level failure that is not a protocol error: an unexpected HTTP status, a
 * body we could not decode, a child process that died. JSON-RPC errors returned by the
 * server arrive as {@link io.modelcontextprotocol.spec.McpError} on the request future
 * instead and never surface as this type.
 */
public class McpTransportException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public McpTransportException(String message) {
    super(message);
  }

  public McpTransportException(String message, Throwable cause) {
    super(message, cause);
  }
}
