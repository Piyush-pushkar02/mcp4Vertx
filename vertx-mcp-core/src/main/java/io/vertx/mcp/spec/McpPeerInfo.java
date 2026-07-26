package io.vertx.mcp.spec;

import io.modelcontextprotocol.spec.McpSchema;

/**
 * Implemented by sessions that know who is on the other end. Lets the server layer build
 * an exchange from a request context without keeping its own session registry.
 */
public interface McpPeerInfo {

  McpSchema.ClientCapabilities clientCapabilities();

  McpSchema.Implementation clientInfo();
}
