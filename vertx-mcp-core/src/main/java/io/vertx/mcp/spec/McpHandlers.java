package io.vertx.mcp.spec;

import io.vertx.core.Future;

/** Handler contracts for inbound JSON-RPC traffic. */
public final class McpHandlers {

  private McpHandlers() {
  }

  /**
   * Handles a request. Return a future of the result object; it will be serialized into
   * the JSON-RPC {@code result} field. Fail the future with
   * {@link io.modelcontextprotocol.spec.McpError} to control the JSON-RPC error code.
   */
  @FunctionalInterface
  public interface RequestHandler {
    Future<Object> handle(McpRequestContext context);
  }

  /** Handles a notification. Nothing is written back. */
  @FunctionalInterface
  public interface NotificationHandler {
    Future<Void> handle(McpRequestContext context);
  }
}
