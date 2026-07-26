package io.vertx.mcp.spec;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.mcp.session.McpStreamableServerSession;

/**
 * Creates a streamable-HTTP session in response to an {@code initialize} request.
 *
 * <p>
 * Session creation is a two-step dance because the initialize response has to go out
 * before the session is considered live: the factory returns both the session and the
 * result to write back.
 */
@FunctionalInterface
public interface McpStreamableSessionFactory {

  Future<InitResult> startSession(McpSchema.InitializeRequest initializeRequest);

  /**
   * @param session the newly created session, already registered
   * @param initializeResult the payload to write back to the client
   */
  record InitResult(McpStreamableServerSession session, McpSchema.InitializeResult initializeResult) {
  }
}
