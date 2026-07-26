package io.vertx.mcp.server.transport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.mcp.McpTransportContext;
import io.vertx.mcp.server.VertxMcpServer;
import io.vertx.mcp.session.McpStreamableServerSession;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpStreamableSessionFactory;
import io.vertx.mcp.spec.McpTransports.McpStreamableServerTransportProvider;

/**
 * The Streamable HTTP server transport (protocol 2025-03-26 and later): one endpoint,
 * three methods.
 *
 * <ul>
 * <li><b>POST</b> carries every client-to-server message. An {@code initialize} request
 * without a session header mints a session and is answered as plain JSON with the new id
 * in {@code Mcp-Session-Id}. A notification or a response to a server-initiated request is
 * acknowledged with {@code 202 Accepted} and an empty body. Any other request opens an SSE
 * response, so progress notifications and reverse requests emitted while the handler runs
 * ride back on the same HTTP response -- which is the whole point of the transport.</li>
 * <li><b>GET</b> opens the standalone listening stream for unsolicited server-to-client
 * traffic, optionally resuming from {@code Last-Event-ID}.</li>
 * <li><b>DELETE</b> terminates the session.</li>
 * </ul>
 *
 * <p>
 * Mount the router the transport hands you:
 *
 * <pre>{@code
 * StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, server);
 * router.route("/mcp*").subRouter(transport.router());
 * }</pre>
 *
 * The router also works standalone as an {@code HttpServer} request handler, because the
 * endpoint is registered both at its configured path and at {@code "/"} (vertx-web strips
 * the mount prefix before a sub-router sees the request).
 */
public final class StreamableHttpServerTransport implements McpStreamableServerTransportProvider {

  private static final Logger logger = LoggerFactory.getLogger(StreamableHttpServerTransport.class);

  private final Vertx vertx;

  private final JsonRpcCodec codec;

  private final String mcpEndpoint;

  private final Set<String> allowedOrigins;

  private final boolean disallowDelete;

  private final Duration keepAliveInterval;

  private final Function<RoutingContext, McpTransportContext> contextExtractor;

  private final ConcurrentHashMap<String, McpStreamableServerSession> sessions = new ConcurrentHashMap<>();

  private volatile McpStreamableSessionFactory sessionFactory;

  private volatile boolean closing;

  private StreamableHttpServerTransport(Builder builder) {
    this.vertx = builder.vertx;
    this.codec = builder.codec;
    this.mcpEndpoint = builder.mcpEndpoint;
    this.allowedOrigins = Set.copyOf(builder.allowedOrigins);
    this.disallowDelete = builder.disallowDelete;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.contextExtractor = builder.contextExtractor;
    this.sessionFactory = builder.sessionFactory;
  }

  /**
   * Creates a transport wired to {@code server}: it takes the server's session factory and
   * codec, and registers itself as a broadcaster so {@code tools/list_changed} and friends
   * reach every connected client.
   */
  public static StreamableHttpServerTransport create(Vertx vertx, VertxMcpServer server) {
    return builder(vertx, server).build();
  }

  public static Builder builder(Vertx vertx, VertxMcpServer server) {
    return new Builder(vertx, server);
  }

  @Override
  public void setSessionFactory(McpStreamableSessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /** Live sessions, keyed by {@code Mcp-Session-Id}. */
  public int sessionCount() {
    return this.sessions.size();
  }

  /**
   * The routes for this transport. Register it once and mount it; calling this twice
   * builds two independent routers over the same session map, which is rarely what you
   * want.
   */
  public Router router() {
    Router router = Router.router(this.vertx);
    router.route().handler(BodyHandler.create(false));
    HttpTransports.routeBoth(router, HttpMethod.POST, this.mcpEndpoint, this::handlePost);
    HttpTransports.routeBoth(router, HttpMethod.GET, this.mcpEndpoint, this::handleGet);
    HttpTransports.routeBoth(router, HttpMethod.DELETE, this.mcpEndpoint, this::handleDelete);
    return router;
  }

  // --- POST ----------------------------------------------------------------

  private void handlePost(RoutingContext rc) {
    if (!precondition(rc)) {
      return;
    }
    // The spec requires a client to advertise both, because it cannot know in advance
    // whether this POST will be answered with JSON or with an SSE stream.
    if (!HttpTransports.accepts(rc, HttpTransports.APPLICATION_JSON)
        || !HttpTransports.accepts(rc, HttpTransports.TEXT_EVENT_STREAM)) {
      HttpTransports.writeError(rc, this.codec, 406, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Accept header must include both application/json and text/event-stream");
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

    String sessionId = rc.request().getHeader(HttpHeaders.MCP_SESSION_ID);
    McpTransportContext transportContext = this.contextExtractor.apply(rc);

    if (message instanceof JSONRPCRequest request && McpSchema.METHOD_INITIALIZE.equals(request.method())
        && (sessionId == null || sessionId.isBlank())) {
      handleInitialize(rc, request);
      return;
    }

    // Absent header and unknown session are different failures: the first is a malformed
    // request, the second means the server forgot a session the client still believes in
    // and should re-initialize. GET and DELETE draw the same distinction below.
    if (sessionId == null || sessionId.isBlank()) {
      HttpTransports.writeError(rc, this.codec, 400, requestIdOf(message), McpSchema.ErrorCodes.INVALID_REQUEST,
          "Mcp-Session-Id header is required");
      return;
    }
    McpStreamableServerSession session = this.sessions.get(sessionId);
    if (session == null) {
      HttpTransports.writeError(rc, this.codec, 404, requestIdOf(message), HttpTransports.SESSION_NOT_FOUND,
          "Session not found");
      return;
    }

    if (message instanceof JSONRPCNotification notification) {
      session.handleNotification(notification, transportContext)
        .onFailure(err -> logger.warn("Notification {} failed on session {}", notification.method(), sessionId, err));
      accepted(rc);
      return;
    }
    if (message instanceof JSONRPCResponse response) {
      session.handleClientResponse(response)
        .onFailure(err -> logger.warn("Client response {} failed on session {}", response.id(), sessionId, err));
      accepted(rc);
      return;
    }
    if (message instanceof JSONRPCRequest request) {
      openResponseStream(rc, session, request, transportContext);
      return;
    }
    HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
        "Unsupported JSON-RPC message type");
  }

  /**
   * Answers {@code initialize} as plain JSON rather than SSE. The client needs the session
   * id before it can open anything else, and there is nothing to stream yet.
   */
  private void handleInitialize(RoutingContext rc, JSONRPCRequest request) {
    McpStreamableSessionFactory factory = this.sessionFactory;
    if (factory == null) {
      HttpTransports.writeError(rc, this.codec, 500, request.id(), McpSchema.ErrorCodes.INTERNAL_ERROR,
          "No session factory configured on this transport");
      return;
    }
    McpSchema.InitializeRequest initializeRequest;
    try {
      initializeRequest = this.codec.unmarshal(request.params(), McpSchema.InitializeRequest.class);
    }
    catch (RuntimeException e) {
      HttpTransports.writeError(rc, this.codec, 400, request.id(), McpSchema.ErrorCodes.INVALID_PARAMS,
          "Invalid initialize params: " + e.getMessage());
      return;
    }
    factory.startSession(initializeRequest).onSuccess(init -> {
      this.sessions.put(init.session().id(), init.session());
      JSONRPCResponse body = new JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), init.initializeResult(),
          null);
      rc.response()
        .setStatusCode(200)
        .putHeader("Content-Type", HttpTransports.APPLICATION_JSON)
        .putHeader(HttpHeaders.MCP_SESSION_ID, init.session().id())
        .end(this.codec.encodeToBuffer(body));
    }).onFailure(err -> {
      logger.error("Failed to start a session", err);
      HttpTransports.writeError(rc, this.codec, 500, request.id(), McpSchema.ErrorCodes.INTERNAL_ERROR,
          "Failed to start session: " + err.getMessage());
    });
  }

  private void openResponseStream(RoutingContext rc, McpStreamableServerSession session, JSONRPCRequest request,
      McpTransportContext transportContext) {
    HttpServerResponse response = rc.response();
    HttpTransports.openSseResponse(response);
    SseWriter writer = new SseWriter(this.vertx, response, this.codec);
    response.closeHandler(v -> writer.markClosed());
    // The session owns the stream's lifetime: it writes the result plus anything the
    // handler emitted, then closes the transport, which ends this HTTP response.
    session.responseStream(request, writer, transportContext).onFailure(err -> {
      logger.error("Request {} failed on session {}", request.id(), session.id(), err);
      writer.close();
    });
  }

  // --- GET -----------------------------------------------------------------

  private void handleGet(RoutingContext rc) {
    if (!precondition(rc)) {
      return;
    }
    if (!HttpTransports.accepts(rc, HttpTransports.TEXT_EVENT_STREAM)) {
      HttpTransports.writeError(rc, this.codec, 406, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Accept header must include text/event-stream");
      return;
    }
    String sessionId = rc.request().getHeader(HttpHeaders.MCP_SESSION_ID);
    if (sessionId == null || sessionId.isBlank()) {
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Mcp-Session-Id header is required");
      return;
    }
    McpStreamableServerSession session = this.sessions.get(sessionId);
    if (session == null) {
      HttpTransports.writeError(rc, this.codec, 404, null, HttpTransports.SESSION_NOT_FOUND, "Session not found");
      return;
    }

    HttpServerResponse response = rc.response();
    HttpTransports.openSseResponse(response);
    SseWriter writer = new SseWriter(this.vertx, response, this.codec);
    response.closeHandler(v -> writer.markClosed());
    // Flush the response head so the client observes the 200 without waiting for traffic.
    writer.sendComment("connected").onFailure(err -> logger.debug("SSE stream closed before it opened", err));

    String lastEventId = rc.request().getHeader(HttpHeaders.LAST_EVENT_ID);
    Future<McpStreamableServerSession.SessionStream> stream;
    if (lastEventId != null && !lastEventId.isBlank()) {
      stream = session.resumeStream(writer, lastEventId).recover(err -> {
        // Resumption is best-effort: an expired or unknown event id must not cost the
        // client its stream, it just costs it the replay.
        logger.warn("Cannot replay session {} from Last-Event-ID {}; opening a fresh listening stream", sessionId,
            lastEventId, err);
        return session.listeningStream(writer);
      });
    }
    else {
      stream = session.listeningStream(writer);
    }

    bindListeningStream(writer, stream, sessionId);
  }

  private void bindListeningStream(SseWriter writer, Future<McpStreamableServerSession.SessionStream> streamFuture,
      String sessionId) {
    AtomicReference<McpStreamableServerSession.SessionStream> bound = new AtomicReference<>();
    writer.onClose(() -> {
      McpStreamableServerSession.SessionStream stream = bound.getAndSet(null);
      if (stream != null) {
        stream.close();
      }
    });
    streamFuture.onSuccess(stream -> {
      // The client may have hung up while the session context was being reached.
      if (writer.isClosed()) {
        stream.close();
        return;
      }
      bound.set(stream);
      if (this.keepAliveInterval != null) {
        long timerId = this.vertx.setPeriodic(this.keepAliveInterval.toMillis(),
            id -> writer.sendComment("keep-alive").onFailure(err -> writer.markClosed()));
        writer.onClose(() -> this.vertx.cancelTimer(timerId));
      }
    }).onFailure(err -> {
      logger.error("Failed to open a listening stream for session {}", sessionId, err);
      writer.close();
    });
  }

  // --- DELETE --------------------------------------------------------------

  private void handleDelete(RoutingContext rc) {
    if (!precondition(rc)) {
      return;
    }
    if (this.disallowDelete) {
      HttpTransports.writeError(rc, this.codec, 405, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Session termination is disabled on this server");
      return;
    }
    String sessionId = rc.request().getHeader(HttpHeaders.MCP_SESSION_ID);
    if (sessionId == null || sessionId.isBlank()) {
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Mcp-Session-Id header is required");
      return;
    }
    McpStreamableServerSession session = this.sessions.remove(sessionId);
    if (session == null) {
      HttpTransports.writeError(rc, this.codec, 404, null, HttpTransports.SESSION_NOT_FOUND, "Session not found");
      return;
    }
    session.close().onComplete(ar -> {
      if (ar.failed()) {
        logger.warn("Error closing session {}", sessionId, ar.cause());
      }
      if (!rc.response().ended()) {
        rc.response().setStatusCode(200).end();
      }
    });
  }

  // --- provider ------------------------------------------------------------

  @Override
  public Future<Void> notifyClients(String method, Object params) {
    List<Future<?>> sends = new ArrayList<>();
    for (McpStreamableServerSession session : this.sessions.values()) {
      sends.add(session.sendNotification(method, params).recover(err -> {
        // A session with no open GET stream cannot receive unsolicited traffic. That is
        // normal, not an error worth failing the broadcast over.
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
    for (McpStreamableServerSession session : this.sessions.values()) {
      closes.add(session.close().recover(err -> Future.succeededFuture()));
    }
    this.sessions.clear();
    return Future.all(closes).mapEmpty();
  }

  // --- shared checks -------------------------------------------------------

  /** Runs the checks every method shares. Writes the error response and returns false. */
  private boolean precondition(RoutingContext rc) {
    if (this.closing) {
      HttpTransports.writeError(rc, this.codec, 503, null, McpSchema.ErrorCodes.INTERNAL_ERROR,
          "Server is shutting down");
      return false;
    }
    if (!HttpTransports.originAllowed(rc, this.allowedOrigins)) {
      HttpTransports.writeError(rc, this.codec, 403, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Origin not allowed");
      return false;
    }
    if (!HttpTransports.protocolVersionSupported(rc)) {
      HttpTransports.writeError(rc, this.codec, 400, null, McpSchema.ErrorCodes.INVALID_REQUEST,
          "Unsupported MCP-Protocol-Version: " + rc.request().getHeader(HttpHeaders.PROTOCOL_VERSION));
      return false;
    }
    return true;
  }

  private static void accepted(RoutingContext rc) {
    if (!rc.response().ended()) {
      rc.response().setStatusCode(202).end();
    }
  }

  private static Object requestIdOf(JSONRPCMessage message) {
    if (message instanceof JSONRPCRequest request) {
      return request.id();
    }
    if (message instanceof JSONRPCResponse response) {
      return response.id();
    }
    return null;
  }

  /** Options for {@link StreamableHttpServerTransport}. */
  public static final class Builder {

    private final Vertx vertx;

    private final VertxMcpServer server;

    private JsonRpcCodec codec;

    private String mcpEndpoint = "/mcp";

    private final Set<String> allowedOrigins = new LinkedHashSet<>();

    private boolean disallowDelete;

    private Duration keepAliveInterval;

    private Function<RoutingContext, McpTransportContext> contextExtractor = HttpTransports.defaultContextExtractor();

    private McpStreamableSessionFactory sessionFactory;

    private Builder(Vertx vertx, VertxMcpServer server) {
      this.vertx = vertx;
      this.server = server;
      this.codec = server.codec();
      this.sessionFactory = server.streamableSessionFactory();
    }

    /** Path of the single MCP endpoint. Defaults to {@code /mcp}. */
    public Builder mcpEndpoint(String mcpEndpoint) {
      this.mcpEndpoint = mcpEndpoint;
      return this;
    }

    /**
     * Origins a browser client may connect from. Empty (the default) disables the check;
     * set it for any server bound to localhost, where DNS rebinding is a real attack.
     */
    public Builder allowedOrigins(String... origins) {
      this.allowedOrigins.addAll(List.of(origins));
      return this;
    }

    public Builder allowedOrigins(java.util.Collection<String> origins) {
      this.allowedOrigins.addAll(origins);
      return this;
    }

    /** Rejects {@code DELETE} with 405, for deployments where sessions expire on their own. */
    public Builder disallowDelete(boolean disallowDelete) {
      this.disallowDelete = disallowDelete;
      return this;
    }

    /**
     * How often to emit an SSE comment on an idle listening stream. Null (the default)
     * disables it; set it below the idle timeout of whatever proxy sits in front.
     */
    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    /** Chooses what transport-level metadata reaches handlers. */
    public Builder contextExtractor(Function<RoutingContext, McpTransportContext> contextExtractor) {
      this.contextExtractor = contextExtractor;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    public StreamableHttpServerTransport build() {
      StreamableHttpServerTransport transport = new StreamableHttpServerTransport(this);
      this.server.registerBroadcaster(transport::notifyClients);
      return transport;
    }
  }
}
