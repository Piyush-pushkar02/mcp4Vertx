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
package io.github.mcp4vertx.server.transport;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.spec.JsonRpcCodec;
import io.github.mcp4vertx.spec.McpStatelessHandler;
import io.github.mcp4vertx.spec.McpTransports.McpStatelessServerTransport;

/**
 * A session-less MCP transport: every POST is answered inline and nothing at all is
 * retained between calls.
 *
 * <p>
 * That rules out server-to-client traffic -- no SSE, no reverse requests, no
 * notifications -- but it also removes the only reason streamable HTTP needs sticky
 * routing. This is the shape to deploy on Lambda, Cloud Run or any horizontally scaled
 * fleet where consecutive requests from one client land on different instances.
 *
 * <pre>{@code
 * router.route("/mcp*").subRouter(StatelessHttpServerTransport.create(vertx, server).router());
 * }</pre>
 */
public final class StatelessHttpServerTransport implements McpStatelessServerTransport {

  private static final Logger logger = LoggerFactory.getLogger(StatelessHttpServerTransport.class);

  private final Vertx vertx;

  private final JsonRpcCodec codec;

  private final String mcpEndpoint;

  private final Set<String> allowedOrigins;

  private final Function<RoutingContext, McpTransportContext> contextExtractor;

  private volatile McpStatelessHandler handler;

  private volatile boolean closing;

  private StatelessHttpServerTransport(Builder builder) {
    this.vertx = builder.vertx;
    this.codec = builder.codec;
    this.mcpEndpoint = builder.mcpEndpoint;
    this.allowedOrigins = Set.copyOf(builder.allowedOrigins);
    this.contextExtractor = builder.contextExtractor;
    this.handler = builder.handler;
  }

  public static StatelessHttpServerTransport create(Vertx vertx, VertxMcpServer server) {
    return builder(vertx, server).build();
  }

  public static Builder builder(Vertx vertx, VertxMcpServer server) {
    return new Builder(vertx, server);
  }

  @Override
  public void setHandler(McpStatelessHandler handler) {
    this.handler = handler;
  }

  /** See {@link StreamableHttpServerTransport#router()} for how mounting works. */
  public Router router() {
    Router router = Router.router(this.vertx);
    router.route().handler(BodyHandler.create(false));
    HttpTransports.routeBoth(router, HttpMethod.POST, this.mcpEndpoint, this::handlePost);
    HttpTransports.routeBoth(router, HttpMethod.GET, this.mcpEndpoint, this::methodNotAllowed);
    HttpTransports.routeBoth(router, HttpMethod.DELETE, this.mcpEndpoint, this::methodNotAllowed);
    return router;
  }

  private void methodNotAllowed(RoutingContext rc) {
    HttpTransports.writeError(rc, this.codec, 405, null, McpSchema.ErrorCodes.INVALID_REQUEST,
        "This server runs the stateless transport; only POST is supported");
  }

  private void handlePost(RoutingContext rc) {
    if (this.closing) {
      HttpTransports.writeError(rc, this.codec, 503, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "Server is shutting down");
      return;
    }
    if (!HttpTransports.originAllowed(rc, this.allowedOrigins)) {
      HttpTransports.writeError(rc, this.codec, 403, null, McpSchema.ErrorCodes.INVALID_REQUEST, "Origin not allowed");
      return;
    }
    if (!HttpTransports.protocolVersionSupported(rc)) {
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Unsupported MCP-Protocol-Version: " + rc.request().getHeader(HttpHeaders.PROTOCOL_VERSION));
      return;
    }
    McpStatelessHandler current = this.handler;
    if (current == null) {
      HttpTransports.writeError(rc, this.codec, 500, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "No handler configured on this transport");
      return;
    }

    JSONRPCMessage message;
    try {
      Buffer body = rc.body() == null ? null : rc.body().buffer();
      if (body == null || body.length() == 0) {
        throw new IllegalArgumentException("Empty request body");
      }
      message = this.codec.decode(body);
    }
    catch (Exception e) {
      logger.debug("Rejecting unparseable POST body", e);
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.PARSE_ERROR,
          "Invalid JSON-RPC message: " + e.getMessage());
      return;
    }

    McpTransportContext transportContext = this.contextExtractor.apply(rc);
    if (message instanceof JSONRPCRequest request) {
      current.handleRequest(transportContext, request)
        .onSuccess(response -> rc.response()
          .setStatusCode(200)
          .putHeader("Content-Type", HttpTransports.APPLICATION_JSON)
          .end(this.codec.encodeToBuffer(response)))
        .onFailure(err -> {
          logger.error("Stateless request {} failed", request.method(), err);
          HttpTransports.writeError(rc, this.codec, 500, request.id(), McpSchema.ErrorCodes.INTERNAL_ERROR,
              "Failed to handle request: " + err.getMessage());
        });
      return;
    }
    if (message instanceof JSONRPCNotification notification) {
      current.handleNotification(transportContext, notification)
        .onFailure(err -> logger.warn("Stateless notification {} failed", notification.method(), err));
      if (!rc.response().ended()) {
        rc.response().setStatusCode(202).end();
      }
      return;
    }
    // A JSONRPCResponse can only answer a server-initiated request, and this transport
    // never issues one.
    HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
        "The stateless transport accepts requests and notifications only");
  }

  @Override
  public Future<Void> close() {
    this.closing = true;
    return Future.succeededFuture();
  }

  /** Options for {@link StatelessHttpServerTransport}. */
  public static final class Builder {

    private final Vertx vertx;

    private JsonRpcCodec codec;

    private String mcpEndpoint = "/mcp";

    private final Set<String> allowedOrigins = new LinkedHashSet<>();

    private Function<RoutingContext, McpTransportContext> contextExtractor = HttpTransports.defaultContextExtractor();

    private McpStatelessHandler handler;

    private Builder(Vertx vertx, VertxMcpServer server) {
      this.vertx = vertx;
      this.codec = server.codec();
      this.handler = server.statelessHandler();
    }

    public Builder mcpEndpoint(String mcpEndpoint) {
      this.mcpEndpoint = mcpEndpoint;
      return this;
    }

    public Builder allowedOrigins(String... origins) {
      this.allowedOrigins.addAll(List.of(origins));
      return this;
    }

    public Builder allowedOrigins(java.util.Collection<String> origins) {
      this.allowedOrigins.addAll(origins);
      return this;
    }

    public Builder contextExtractor(Function<RoutingContext, McpTransportContext> contextExtractor) {
      this.contextExtractor = contextExtractor;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    public StatelessHttpServerTransport build() {
      return new StatelessHttpServerTransport(this);
    }
  }
}
