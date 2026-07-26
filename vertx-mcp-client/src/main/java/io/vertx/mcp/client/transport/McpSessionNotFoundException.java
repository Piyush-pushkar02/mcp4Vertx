package io.vertx.mcp.client.transport;

/**
 * The server no longer knows the {@code Mcp-Session-Id} we are presenting -- it answered
 * {@code 404 Not Found}.
 *
 * <p>
 * This is separated from every other transport failure because the recovery is specific
 * and mechanical: per the streamable HTTP specification the client must start a new
 * session by sending {@code initialize} again, without the stale session header. Callers
 * catch this type to drive that re-handshake instead of surfacing an error to the user.
 */
public class McpSessionNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String sessionId;

  public McpSessionNotFoundException(String sessionId, String message) {
    super(message);
    this.sessionId = sessionId;
  }

  /** The session id the server rejected, or {@code null} if we had not been issued one. */
  public String sessionId() {
    return this.sessionId;
  }
}
