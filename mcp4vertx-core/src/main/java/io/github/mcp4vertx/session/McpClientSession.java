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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.github.mcp4vertx.spec.McpTransports.McpClientTransport;

/**
 * The client end of the conversation. Symmetric to {@link McpServerSession}: it also
 * serves inbound requests, because MCP servers call back into clients for sampling,
 * elicitation and {@code roots/list}.
 */
public class McpClientSession extends AbstractMcpSession {

  private static final Logger logger = LoggerFactory.getLogger(McpClientSession.class);

  private final McpClientTransport transport;

  private final String id;

  public McpClientSession(Vertx vertx, String id, McpClientTransport transport, JsonRpcCodec codec,
      Duration requestTimeout, Map<String, McpHandlers.RequestHandler> requestHandlers,
      Map<String, McpHandlers.NotificationHandler> notificationHandlers) {
    super(vertx, codec, requestTimeout, requestHandlers, notificationHandlers);
    this.id = id;
    this.transport = transport;
  }

  /** Connects the transport and starts routing inbound messages into this session. */
  public Future<Void> connect() {
    return this.transport.connect(message -> Contexts.run(this.context, () -> handle(message)));
  }

  @Override
  protected String idPrefix() {
    return this.id;
  }

  @Override
  protected String sessionId() {
    return this.id;
  }

  @Override
  protected Future<Void> write(JSONRPCMessage message) {
    return this.transport.sendMessage(message);
  }

  private void handle(JSONRPCMessage message) {
    if (message instanceof JSONRPCResponse response) {
      handleResponse(response);
    }
    else if (message instanceof JSONRPCNotification notification) {
      invokeNotificationHandler(notification, McpTransportContext.EMPTY);
    }
    else if (message instanceof JSONRPCRequest request) {
      invokeRequestHandler(request, McpTransportContext.EMPTY, this)
        .compose(response -> response == null ? Future.succeededFuture() : write(response))
        .onFailure(err -> logger.warn("Failed to answer server request {}", request.method(), err));
    }
  }

  @Override
  protected Future<Void> doClose() {
    return this.transport.close();
  }
}
