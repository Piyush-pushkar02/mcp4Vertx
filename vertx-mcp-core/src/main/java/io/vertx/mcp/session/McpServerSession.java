package io.vertx.mcp.session;

import java.time.Duration;
import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.McpTransportContext;
import io.vertx.mcp.internal.Contexts;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpHandlers;
import io.vertx.mcp.spec.McpPeerInfo;
import io.vertx.mcp.spec.McpTransports.McpServerTransport;

/**
 * A session over a single bidirectional transport: legacy HTTP+SSE, or stdio. Everything
 * -- responses, server-initiated requests, notifications -- travels on the one stream.
 */
public class McpServerSession extends AbstractMcpSession implements McpPeerInfo {

  private final String id;

  private final McpServerTransport transport;

  private volatile McpSchema.ClientCapabilities clientCapabilities;

  private volatile McpSchema.Implementation clientInfo;

  private volatile boolean initialized;

  public McpServerSession(Vertx vertx, String id, McpServerTransport transport, JsonRpcCodec codec,
      Duration requestTimeout, Map<String, McpHandlers.RequestHandler> requestHandlers,
      Map<String, McpHandlers.NotificationHandler> notificationHandlers) {
    super(vertx, codec, requestTimeout, requestHandlers, notificationHandlers);
    this.id = id;
    this.transport = transport;
  }

  public String id() {
    return this.id;
  }

  @Override
  protected String sessionId() {
    return this.id;
  }

  @Override
  protected String idPrefix() {
    return this.id;
  }

  @Override
  protected Future<Void> write(JSONRPCMessage message) {
    return this.transport.sendMessage(message);
  }

  @Override
  public McpSchema.ClientCapabilities clientCapabilities() {
    return this.clientCapabilities;
  }

  @Override
  public McpSchema.Implementation clientInfo() {
    return this.clientInfo;
  }

  public void clientInfo(McpSchema.ClientCapabilities capabilities, McpSchema.Implementation info) {
    this.clientCapabilities = capabilities;
    this.clientInfo = info;
  }

  public boolean isInitialized() {
    return this.initialized;
  }

  public void markInitialized() {
    this.initialized = true;
  }

  /**
   * Routes one inbound message. Hops onto the session context first, so the transport can
   * call this from any event loop.
   */
  public Future<Void> handle(JSONRPCMessage message, McpTransportContext transportContext) {
    return Contexts.on(this.context, () -> {
      if (message instanceof JSONRPCResponse response) {
        handleResponse(response);
        return Future.succeededFuture();
      }
      if (message instanceof JSONRPCNotification notification) {
        return invokeNotificationHandler(notification, transportContext);
      }
      if (message instanceof JSONRPCRequest request) {
        return invokeRequestHandler(request, transportContext, this)
          .compose(response -> response == null ? Future.succeededFuture() : write(response));
      }
      return Future.failedFuture(new IllegalArgumentException("Unknown message type: " + message.getClass()));
    });
  }

  @Override
  protected Future<Void> doClose() {
    return this.transport.close();
  }
}
