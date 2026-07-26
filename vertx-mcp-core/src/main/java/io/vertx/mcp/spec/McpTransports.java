package io.vertx.mcp.spec;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Future;

/**
 * The Vert.x transport SPI.
 *
 * <p>
 * Everything here returns {@link Future}. There is no {@code Mono}, no {@code Publisher},
 * and no scheduler indirection: a transport writes on the Vert.x context it was created
 * on, and the returned future completes on that same context. That is the whole point of
 * this SDK -- {@code Vertx.currentContext()} is never {@code null} in a handler, so
 * context-local storage, tracing propagation and ordered task execution all keep working.
 *
 * <p>
 * Grouped in one file because these are five tiny, tightly-related contracts.
 */
public final class McpTransports {

  private McpTransports() {
  }

  /** Base contract: something you can write a JSON-RPC message to, and close. */
  public interface McpTransport {

    /**
     * Writes a single JSON-RPC message. The returned future completes when the bytes have
     * been handed to the underlying {@code WriteStream}, respecting its backpressure --
     * if the write queue is full the future does not complete until the stream drains.
     */
    Future<Void> sendMessage(JSONRPCMessage message);

    /** Closes the transport, flushing anything pending. Idempotent. */
    Future<Void> close();
  }

  /** Server side of a single connection or stream. */
  public interface McpServerTransport extends McpTransport {
  }

  /**
   * A server transport that can tag outbound messages with an SSE event id, so a client
   * that reconnects with {@code Last-Event-ID} can be replayed from where it dropped.
   */
  public interface McpStreamableServerTransport extends McpServerTransport {

    Future<Void> sendMessage(JSONRPCMessage message, String messageId);

    @Override
    default Future<Void> sendMessage(JSONRPCMessage message) {
      return sendMessage(message, null);
    }
  }

  /**
   * Owns the listening socket / endpoint and mints a session per connected client. The
   * classic HTTP+SSE (2024-11-05) transport implements this.
   */
  public interface McpServerTransportProvider {

    void setSessionFactory(McpSessionFactory sessionFactory);

    /** Broadcasts a notification to every live session. */
    Future<Void> notifyClients(String method, Object params);

    Future<Void> close();
  }

  /**
   * Streamable HTTP (2025-03-26 and later): one endpoint, POST for requests, GET for the
   * standalone server-to-client stream, DELETE to terminate the session.
   */
  public interface McpStreamableServerTransportProvider {

    void setSessionFactory(McpStreamableSessionFactory sessionFactory);

    Future<Void> notifyClients(String method, Object params);

    Future<Void> close();
  }

  /**
   * A transport with no session state at all: every POST is answered inline and nothing is
   * retained between calls. The right shape for serverless and horizontally scaled
   * deployments where no sticky routing exists.
   */
  public interface McpStatelessServerTransport {

    void setHandler(McpStatelessHandler handler);

    Future<Void> close();
  }

  /** Client side. */
  public interface McpClientTransport extends McpTransport {

    /**
     * Connects and installs the inbound message handler. The handler is invoked on the
     * transport's Vert.x context for every message the server sends.
     */
    Future<Void> connect(java.util.function.Consumer<JSONRPCMessage> inboundHandler);

    /** Called when the transport fails outside the scope of any single request. */
    default void exceptionHandler(java.util.function.Consumer<Throwable> handler) {
    }

    /**
     * Protocol versions this transport can speak, newest last. Used during the initialize
     * handshake.
     */
    default java.util.List<String> protocolVersions() {
      return java.util.List.of(io.modelcontextprotocol.spec.ProtocolVersions.MCP_2025_06_18);
    }
  }
}
