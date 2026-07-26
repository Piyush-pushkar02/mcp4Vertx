package io.vertx.mcp.spec;

/**
 * Protocol method names the upstream {@code McpSchema} does not expose as constants.
 */
public final class McpMethods {

  /** Client -> server: abandon an in-flight request. */
  public static final String NOTIFICATION_CANCELLED = "notifications/cancelled";

  private McpMethods() {
  }
}
