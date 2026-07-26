/*
 * Copyright 2026 the mcp4vertx authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.mcp4vertx.session;

import java.time.Duration;
import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.internal.Contexts;
import io.github.mcp4vertx.spec.JsonRpcCodec;
import io.github.mcp4vertx.spec.McpHandlers;
import io.github.mcp4vertx.spec.McpPeerInfo;
import io.github.mcp4vertx.spec.McpTransports.McpServerTransport;

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
