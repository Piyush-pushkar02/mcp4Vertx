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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.session.McpServerSession;
import io.github.mcp4vertx.spec.JsonRpcCodec;
import io.github.mcp4vertx.spec.McpSessionFactory;
import io.github.mcp4vertx.spec.McpTransports.McpServerTransportProvider;

/**
 * The legacy HTTP+SSE server transport (protocol 2024-11-05), kept for clients that have
 * not moved to Streamable HTTP.
 *
 * <p>
 * It splits the conversation across two endpoints: a long-lived {@code GET /sse} stream
 * carrying everything the server says, and a {@code POST /message} for everything the
 * client says. The client learns where to POST from the very first SSE frame, an
 * {@code endpoint} event whose data is the message URL with the session id appended as a
 * query parameter -- that handshake is what the 2024-11-05 spec mandates, and it is the
 * reason the session id is not an HTTP header here.
 *
 * <pre>{@code
 * SseServerTransport transport = SseServerTransport.create(vertx, server);
 * router.route("/*").subRouter(transport.router());
 * }</pre>
 */
public final class SseServerTransport implements McpServerTransportProvider {

  private static final Logger logger = LoggerFactory.getLogger(SseServerTransport.class);

  /** Query parameter naming the session on the message endpoint. */
  public static final String SESSION_ID_PARAM = "sessionId";

  private final Vertx vertx;

  private final JsonRpcCodec codec;

  private final String sseEndpoint;

  private final String messageEndpoint;

  private final String baseUrl;

  private final Set<String> allowedOrigins;

  private final Duration keepAliveInterval;

  private final Function<RoutingContext, McpTransportContext> contextExtractor;

  private final ConcurrentHashMap<String, McpServerSession> sessions = new ConcurrentHashMap<>();

  private volatile McpSessionFactory sessionFactory;

  private volatile boolean closing;

  private SseServerTransport(Builder builder) {
    this.vertx = builder.vertx;
    this.codec = builder.codec;
    this.sseEndpoint = builder.sseEndpoint;
    this.messageEndpoint = builder.messageEndpoint;
    this.baseUrl = builder.baseUrl;
    this.allowedOrigins = Set.copyOf(builder.allowedOrigins);
    this.keepAliveInterval = builder.keepAliveInterval;
    this.contextExtractor = builder.contextExtractor;
    this.sessionFactory = builder.sessionFactory;
  }

  public static SseServerTransport create(Vertx vertx, VertxMcpServer server) {
    return builder(vertx, server).build();
  }

  public static Builder builder(Vertx vertx, VertxMcpServer server) {
    return new Builder(vertx, server);
  }

  @Override
  public void setSessionFactory(McpSessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public int sessionCount() {
    return this.sessions.size();
  }

  /**
   * The two routes for this transport. Unlike the streamable transport these are two
   * distinct paths, so mount the router at a prefix that contains both -- typically the
   * application root.
   */
  public Router router() {
    Router router = Router.router(this.vertx);
    router.route().handler(BodyHandler.create(false));
    router.get(this.sseEndpoint).handler(this::handleSse);
    router.post(this.messageEndpoint).handler(this::handleMessage);
    return router;
  }

  private void handleSse(RoutingContext rc) {
    if (this.closing) {
      HttpTransports.writeError(rc, this.codec, 503, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "Server is shutting down");
      return;
    }
    if (!HttpTransports.originAllowed(rc, this.allowedOrigins)) {
      HttpTransports.writeError(rc, this.codec, 403, null, McpSchema.ErrorCodes.INVALID_REQUEST, "Origin not allowed");
      return;
    }
    McpSessionFactory factory = this.sessionFactory;
    if (factory == null) {
      HttpTransports.writeError(rc, this.codec, 500, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "No session factory configured on this transport");
      return;
    }

    String sessionId = UUID.randomUUID().toString();
    HttpServerResponse response = rc.response();
    HttpTransports.openSseResponse(response);
    SseWriter writer = new SseWriter(this.vertx, response, this.codec);
    response.closeHandler(v -> writer.markClosed());

    // The endpoint event must be the first thing on the wire: until the client has it, it
    // has nowhere to POST and the conversation cannot start.
    writer.sendEvent(SseWriter.ENDPOINT_EVENT, endpointUrl(sessionId), null)
      .onFailure(err -> logger.debug("SSE stream {} closed before the endpoint event went out", sessionId, err));

    McpServerSession session = factory.create(sessionId, writer);
    this.sessions.put(sessionId, session);
    writer.onClose(() -> {
      this.sessions.remove(sessionId);
      session.close();
    });

    if (this.keepAliveInterval != null) {
      long timerId = this.vertx.setPeriodic(this.keepAliveInterval.toMillis(),
          id -> writer.sendComment("keep-alive").onFailure(err -> writer.markClosed()));
      writer.onClose(() -> this.vertx.cancelTimer(timerId));
    }
  }

  private String endpointUrl(String sessionId) {
    String base = this.baseUrl.endsWith("/") ? this.baseUrl.substring(0, this.baseUrl.length() - 1) : this.baseUrl;
    return base + this.messageEndpoint + "?" + SESSION_ID_PARAM + "=" + sessionId;
  }

  private void handleMessage(RoutingContext rc) {
    if (this.closing) {
      HttpTransports.writeError(rc, this.codec, 503, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "Server is shutting down");
      return;
    }
    if (!HttpTransports.originAllowed(rc, this.allowedOrigins)) {
      HttpTransports.writeError(rc, this.codec, 403, null, McpSchema.ErrorCodes.INVALID_REQUEST, "Origin not allowed");
      return;
    }
    String sessionId = rc.request().getParam(SESSION_ID_PARAM);
    if (sessionId == null || sessionId.isBlank()) {
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "sessionId query parameter is required");
      return;
    }
    McpServerSession session = this.sessions.get(sessionId);
    if (session == null) {
      HttpTransports.writeError(rc, this.codec, 404, null, HttpTransports.SESSION_NOT_FOUND, "Session not found");
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
      logger.debug("Rejecting unparseable POST body on session {}", sessionId, e);
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.PARSE_ERROR,
          "Invalid JSON-RPC message: " + e.getMessage());
      return;
    }

    // The reply -- if there is one -- goes out on the SSE stream, not on this response,
    // so the POST is acknowledged as soon as the message is handed to the session.
    session.handle(message, this.contextExtractor.apply(rc))
      .onFailure(err -> logger.warn("Message handling failed on session {}", sessionId, err));
    if (!rc.response().ended()) {
      rc.response().setStatusCode(202).end();
    }
  }

  @Override
  public Future<Void> notifyClients(String method, Object params) {
    List<Future<?>> sends = new ArrayList<>();
    for (McpServerSession session : this.sessions.values()) {
      sends.add(session.sendNotification(method, params).recover(err -> {
        logger.debug("Broadcast of {} did not reach session {}", method, session.id(), err);
        return Future.succeededFuture();
      }));
    }
    return Future.all(sends).mapEmpty();
  }

  @Override
  public Future<Void> close() {
    this.closing = true;
    List<Future<?>> closes = new ArrayList<>();
    for (McpServerSession session : this.sessions.values()) {
      closes.add(session.close().recover(err -> Future.succeededFuture()));
    }
    this.sessions.clear();
    return Future.all(closes).mapEmpty();
  }

  /** Options for {@link SseServerTransport}. */
  public static final class Builder {

    private final Vertx vertx;

    private final VertxMcpServer server;

    private JsonRpcCodec codec;

    private String sseEndpoint = "/sse";

    private String messageEndpoint = "/message";

    private String baseUrl = "";

    private final Set<String> allowedOrigins = new LinkedHashSet<>();

    private Duration keepAliveInterval;

    private Function<RoutingContext, McpTransportContext> contextExtractor = HttpTransports.defaultContextExtractor();

    private McpSessionFactory sessionFactory;

    private Builder(Vertx vertx, VertxMcpServer server) {
      this.vertx = vertx;
      this.server = server;
      this.codec = server.codec();
      this.sessionFactory = server.sessionFactory();
    }

    /** Path clients GET to open the event stream. Defaults to {@code /sse}. */
    public Builder sseEndpoint(String sseEndpoint) {
      this.sseEndpoint = sseEndpoint;
      return this;
    }

    /** Path clients POST messages to. Defaults to {@code /message}. */
    public Builder messageEndpoint(String messageEndpoint) {
      this.messageEndpoint = messageEndpoint;
      return this;
    }

    /**
     * Prefix prepended to the advertised message endpoint. Needed when the server sits
     * behind a reverse proxy that mounts it under a path the server itself never sees.
     */
    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
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

    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
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

    public SseServerTransport build() {
      SseServerTransport transport = new SseServerTransport(this);
      this.server.registerBroadcaster(transport::notifyClients);
      return transport;
    }
  }
}
