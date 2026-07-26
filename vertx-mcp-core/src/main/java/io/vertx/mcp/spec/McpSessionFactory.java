package io.vertx.mcp.spec;

import io.vertx.mcp.session.McpServerSession;
import io.vertx.mcp.spec.McpTransports.McpServerTransport;

/**
 * Creates a session bound to a freshly connected client. Implemented by the server layer;
 * called by transport providers.
 */
@FunctionalInterface
public interface McpSessionFactory {

  McpServerSession create(String sessionId, McpServerTransport transport);
}
