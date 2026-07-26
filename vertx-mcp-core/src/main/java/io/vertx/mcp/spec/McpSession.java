package io.vertx.mcp.spec;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;

/**
 * A live JSON-RPC conversation with a peer. Both server-side and client-side sessions
 * implement this.
 */
public interface McpSession {

  /**
   * Sends a request and completes when the peer answers. The future fails with
   * {@link io.modelcontextprotocol.spec.McpError} if the peer returns a JSON-RPC error, or
   * with {@link java.util.concurrent.TimeoutException} if the configured request timeout
   * elapses first.
   */
  <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType);

  /** Fire-and-forget. Completes once the bytes are accepted by the transport. */
  Future<Void> sendNotification(String method, Object params);

  default Future<Void> sendNotification(String method) {
    return sendNotification(method, null);
  }

  /** Closes the session and fails any in-flight requests. */
  Future<Void> close();

  /**
   * Whether a {@code notifications/message} at this level should be delivered, per the
   * client's most recent {@code logging/setLevel}.
   */
  default boolean isLoggingLevelEnabled(McpSchema.LoggingLevel level) {
    return true;
  }

  /** Records the client's requested minimum logging level. */
  default void setMinLoggingLevel(McpSchema.LoggingLevel level) {
  }
}
