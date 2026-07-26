package io.vertx.mcp.spec;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.mcp.McpTransportContext;

/**
 * Handles a single request or notification in the stateless transport, where there is no
 * session to route through.
 */
public interface McpStatelessHandler {

  Future<McpSchema.JSONRPCResponse> handleRequest(McpTransportContext context, McpSchema.JSONRPCRequest request);

  Future<Void> handleNotification(McpTransportContext context, McpSchema.JSONRPCNotification notification);
}
