package io.vertx.mcp.client.transport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.mcp.internal.Contexts;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpTransports.McpClientTransport;

/**
 * The streamable HTTP client transport (protocol 2025-03-26 and later): one endpoint,
 * {@code POST} for everything the client sends, an optional standalone {@code GET} stream
 * for everything the server pushes unprompted, and {@code DELETE} to end the session.
 *
 * <p>
 * A {@code POST} may be answered three ways, and all three matter:
 * <ul>
 * <li>{@code 202 Accepted} with no body -- the normal answer to a notification or to our
 * reply to a server-initiated request.</li>
 * <li>{@code 200} with {@code application/json} -- a single JSON-RPC response.</li>
 * <li>{@code 200} with {@code text/event-stream} -- a stream. This is the interesting
 * one: a single {@code tools/call} can push progress notifications and even a
 * {@code sampling/createMessage} request back at us before the result shows up, and every
 * one of those has to reach the session the moment it arrives. The body is therefore
 * parsed incrementally by {@link SseResponseReader} rather than collected.</li>
 * </ul>
 *
 * <p>
 * Built directly on vertx-core's {@link HttpClient} rather than on the Web Client, because
 * the choice above has to be made from the response head: {@code HttpClientRequest#send}
 * hands back the {@link HttpClientResponse} as soon as the status line and headers arrive,
 * which is the only moment at which {@code Mcp-Session-Id} is early enough to be useful
 * and the {@code Content-Type} is known while the body is still streaming.
 *
 * <pre>{@code
 * var transport = StreamableHttpClientTransport.builder(vertx)
 *     .baseUrl("https://example.com")
 *     .endpoint("/mcp")
 *     .requestCustomizer(request -> request.putHeader("Authorization", "Bearer " + token))
 *     .build();
 * }</pre>
 */
public final class StreamableHttpClientTransport implements McpClientTransport, McpProtocolVersionAware {

  private static final Logger logger = LoggerFactory.getLogger(StreamableHttpClientTransport.class);

  private static final String ACCEPT_BOTH = "application/json, text/event-stream";

  private static final String APPLICATION_JSON = "application/json";

  private static final String TEXT_EVENT_STREAM = "text/event-stream";

  private final Vertx vertx;

  private final Context context;

  private final HttpClient httpClient;

  private final boolean ownsHttpClient;

  private final String endpointUri;

  private final Map<String, String> headers;

  private final Function<HttpClientRequest, HttpClientRequest> requestCustomizer;

  private final Duration requestTimeout;

  private final boolean openListeningStream;

  private final int maxReconnectAttempts;

  private final Duration reconnectDelay;

  private final Duration maxReconnectDelay;

  private final boolean terminateSessionOnClose;

  private final List<String> protocolVersions;

  private final JsonRpcCodec codec;

  private Consumer<JSONRPCMessage> inbound;

  private Consumer<Throwable> exceptionHandler;

  private volatile String sessionId;

  private volatile String negotiatedProtocolVersion;

  private boolean closed;

  private boolean listeningRequested;

  private boolean listeningActive;

  private String lastEventId;

  private HttpClientResponse listeningResponse;

  private long reconnectTimer = -1;

  private int reconnectAttempt;

  private StreamableHttpClientTransport(Builder builder) {
    this.vertx = builder.vertx;
    this.context = Contexts.current(builder.vertx);
    this.ownsHttpClient = builder.httpClient == null;
    this.httpClient = this.ownsHttpClient ? builder.vertx.createHttpClient(builder.httpClientOptions)
        : builder.httpClient;
    this.endpointUri = resolve(builder.baseUrl, builder.endpoint);
    this.headers = Map.copyOf(builder.headers);
    this.requestCustomizer = builder.requestCustomizer;
    this.requestTimeout = builder.requestTimeout;
    this.openListeningStream = builder.openListeningStream;
    this.maxReconnectAttempts = builder.maxReconnectAttempts;
    this.reconnectDelay = builder.reconnectDelay;
    this.maxReconnectDelay = builder.maxReconnectDelay;
    this.terminateSessionOnClose = builder.terminateSessionOnClose;
    this.protocolVersions = List.copyOf(builder.protocolVersions);
    this.codec = builder.codec != null ? builder.codec : JsonRpcCodec.defaultCodec();
  }

  public static Builder builder(Vertx vertx) {
    return new Builder(vertx);
  }

  /** The absolute URL every request goes to. */
  public String endpointUri() {
    return this.endpointUri;
  }

  /** The {@code Mcp-Session-Id} the server issued, or {@code null} before initialize. */
  public String sessionId() {
    return this.sessionId;
  }

  @Override
  public List<String> protocolVersions() {
    return this.protocolVersions;
  }

  @Override
  public void negotiatedProtocolVersion(String protocolVersion) {
    this.negotiatedProtocolVersion = protocolVersion;
  }

  @Override
  public void exceptionHandler(Consumer<Throwable> handler) {
    this.exceptionHandler = handler;
  }

  // --- connect -------------------------------------------------------------

  @Override
  public Future<Void> connect(Consumer<JSONRPCMessage> inboundHandler) {
    return Contexts.on(this.context, () -> {
      this.inbound = inboundHandler;
      // The standalone stream is not opened here. It has to carry Mcp-Session-Id, which
      // only exists once initialize has been answered, so opening it is deferred until
      // the first exchange completes.
      this.listeningRequested = this.openListeningStream;
      return Future.succeededFuture();
    });
  }

  // --- outbound ------------------------------------------------------------

  @Override
  public Future<Void> sendMessage(JSONRPCMessage message) {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.failedFuture(new IllegalStateException("Transport is closed"));
      }
      Buffer body = this.codec.encodeToBuffer(message);
      RequestOptions options = requestOptions(HttpMethod.POST).putHeader(HttpHeaders.ACCEPT, ACCEPT_BOTH)
        .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
        // Idle, not absolute: a streaming tool call may legitimately run for minutes while
        // emitting progress, but a connection that has gone silent is dead. The per-request
        // deadline that users actually care about is enforced by the session.
        .setIdleTimeout(this.requestTimeout.toMillis());
      return this.httpClient.request(options)
        .compose(request -> customize(request).send(body))
        .compose(this::onPostResponse);
    });
  }

  /**
   * Dispatches one {@code POST} response from its head. The returned future completes only
   * once the body has been consumed in full, so a caller that awaits it has seen every
   * message the exchange produced.
   */
  private Future<Void> onPostResponse(HttpClientResponse response) {
    captureSessionId(response.getHeader(HttpHeaders.MCP_SESSION_ID));
    int status = response.statusCode();
    if (status == 404) {
      // The server forgot us. Drop the stale id so a re-initialize starts clean.
      response.body();
      String stale = this.sessionId;
      this.sessionId = null;
      return Future.failedFuture(new McpSessionNotFoundException(stale,
          "Server no longer knows session '" + stale + "'; re-initialize to continue"));
    }
    if (status < 200 || status >= 300) {
      // Drained rather than abandoned so the pooled connection stays reusable.
      response.body();
      return Future.failedFuture(new McpTransportException(
          "POST " + this.endpointUri + " answered " + status + " " + response.statusMessage()));
    }
    maybeOpenListeningStream();
    String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
    if (MediaTypes.isEventStream(contentType)) {
      return new SseResponseReader(this.context, response, event -> deliver(event.data())).start();
    }
    return response.body().compose(body -> {
      if (status == 202 || status == 204 || body.length() == 0) {
        return Future.succeededFuture();
      }
      if (contentType != null && !MediaTypes.isJson(contentType)) {
        logger.debug("Ignoring {} response body from {}", contentType, this.endpointUri);
        return Future.succeededFuture();
      }
      deliver(body.toString(StandardCharsets.UTF_8));
      return Future.succeededFuture();
    });
  }

  // --- standalone listening stream -----------------------------------------

  private void maybeOpenListeningStream() {
    if (!this.listeningRequested || this.listeningActive || this.closed) {
      return;
    }
    this.listeningActive = true;
    this.reconnectAttempt = 0;
    openListeningStream();
  }

  private void openListeningStream() {
    RequestOptions options = requestOptions(HttpMethod.GET).putHeader(HttpHeaders.ACCEPT, TEXT_EVENT_STREAM);
    if (this.lastEventId != null) {
      options.putHeader(HttpHeaders.LAST_EVENT_ID, this.lastEventId);
    }
    // Deliberately no idle timeout: a server is entitled to keep this stream open and
    // silent for as long as it likes.
    this.httpClient.request(options)
      .compose(request -> customize(request).send())
      .onSuccess(this::onListeningHead)
      .onFailure(cause -> onListeningEnded(null, cause));
  }

  private void onListeningHead(HttpClientResponse response) {
    if (this.closed || !this.listeningActive) {
      // Raced with close(): nobody is going to read this, and an SSE response never ends
      // by itself.
      response.request().reset();
      return;
    }
    this.listeningResponse = response;
    int status = response.statusCode();
    if (status == 405 || status == 501) {
      // Perfectly legal: the spec lets a server decline the standalone stream. Nothing
      // is broken, there is just nothing to listen to.
      logger.debug("Server declined the standalone SSE stream with {}", status);
      this.listeningResponse = null;
      this.listeningActive = false;
      response.body();
      return;
    }
    if (status == 404) {
      this.listeningResponse = null;
      this.listeningActive = false;
      response.body();
      String stale = this.sessionId;
      this.sessionId = null;
      notifyException(new McpSessionNotFoundException(stale,
          "Server no longer knows session '" + stale + "'; re-initialize to continue"));
      return;
    }
    if (status < 200 || status >= 300 || !MediaTypes.isEventStream(response.getHeader(HttpHeaders.CONTENT_TYPE))) {
      response.body();
      onListeningEnded(null, new McpTransportException(
          "GET " + this.endpointUri + " answered " + status + " " + response.statusMessage()));
      return;
    }
    SseResponseReader reader = new SseResponseReader(this.context, response, this::onListeningEvent);
    reader.start().onComplete(result -> onListeningEnded(reader, result.cause()));
  }

  private void onListeningEvent(SseEvent event) {
    if (event.id() != null) {
      this.lastEventId = event.id();
    }
    // Any traffic proves the endpoint is healthy, so the backoff ladder starts over.
    this.reconnectAttempt = 0;
    deliver(event.data());
  }

  /**
   * The stream is gone, cleanly or not. A clean end is still a reconnect: the spec expects
   * the client to keep a standalone stream up for as long as the session lives.
   */
  private void onListeningEnded(SseResponseReader reader, Throwable cause) {
    this.listeningResponse = null;
    if (this.closed || !this.listeningActive) {
      return;
    }
    if (reader != null && reader.lastEventId() != null) {
      this.lastEventId = reader.lastEventId();
    }
    scheduleReconnect(reader != null ? reader.retryMillis() : null, cause);
  }

  private void scheduleReconnect(Long serverRetryMillis, Throwable cause) {
    if (this.reconnectAttempt >= this.maxReconnectAttempts) {
      this.listeningActive = false;
      notifyException(new McpTransportException(
          "Standalone SSE stream to " + this.endpointUri + " failed after " + this.reconnectAttempt + " retries",
          cause));
      return;
    }
    long base = serverRetryMillis != null ? serverRetryMillis : this.reconnectDelay.toMillis();
    long delay = Math.min(base << Math.min(this.reconnectAttempt, 16), this.maxReconnectDelay.toMillis());
    this.reconnectAttempt++;
    logger.debug("Reconnecting the standalone SSE stream in {}ms (attempt {})", delay, this.reconnectAttempt);
    this.reconnectTimer = this.vertx.setTimer(Math.max(delay, 1), timer -> {
      this.reconnectTimer = -1;
      if (!this.closed && this.listeningActive) {
        openListeningStream();
      }
    });
  }

  // --- close ---------------------------------------------------------------

  @Override
  public Future<Void> close() {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      this.listeningActive = false;
      if (this.reconnectTimer >= 0) {
        this.vertx.cancelTimer(this.reconnectTimer);
        this.reconnectTimer = -1;
      }
      HttpClientResponse listening = this.listeningResponse;
      this.listeningResponse = null;
      if (listening != null) {
        // A standalone SSE response never ends by itself; resetting the stream is the
        // only way to stop waiting on it.
        listening.request().reset();
      }
      return terminateSession().recover(err -> Future.succeededFuture()).andThen(ignored -> closeHttpClient());
    });
  }

  /**
   * Best-effort {@code DELETE} so the server can drop the session's state immediately
   * instead of waiting for its own expiry. A {@code 405} here is a legal answer, so the
   * result is ignored either way.
   */
  private Future<Void> terminateSession() {
    String id = this.sessionId;
    if (!this.terminateSessionOnClose || id == null) {
      return Future.succeededFuture();
    }
    RequestOptions options = requestOptions(HttpMethod.DELETE).setTimeout(this.requestTimeout.toMillis());
    return this.httpClient.request(options)
      .compose(request -> customize(request).send())
      .compose(HttpClientResponse::body)
      .mapEmpty();
  }

  private void closeHttpClient() {
    if (this.ownsHttpClient) {
      this.httpClient.close();
    }
  }

  // --- plumbing ------------------------------------------------------------

  /**
   * Options for one request, with everything this transport always sends already on them.
   * Headers go on the options rather than on the request so the customizer, which runs
   * last, is free to override any of them.
   */
  private RequestOptions requestOptions(HttpMethod method) {
    RequestOptions options = new RequestOptions().setMethod(method).setAbsoluteURI(this.endpointUri);
    // The Web Client this transport used to be built on followed redirects by default;
    // keeping that on means an endpoint that has moved still works.
    options.setFollowRedirects(true);
    this.headers.forEach(options::putHeader);
    String id = this.sessionId;
    if (id != null) {
      options.putHeader(HttpHeaders.MCP_SESSION_ID, id);
    }
    String version = this.negotiatedProtocolVersion;
    if (version != null) {
      options.putHeader(HttpHeaders.PROTOCOL_VERSION, version);
    }
    return options;
  }

  private HttpClientRequest customize(HttpClientRequest request) {
    return this.requestCustomizer != null ? this.requestCustomizer.apply(request) : request;
  }

  private void captureSessionId(String id) {
    if (id == null || id.isEmpty() || id.equals(this.sessionId)) {
      return;
    }
    this.sessionId = id;
    // Now that the session exists the standalone stream can carry its header, so it is
    // safe to open.
    Contexts.run(this.context, this::maybeOpenListeningStream);
  }

  private void deliver(String json) {
    if (json == null || json.isBlank()) {
      return;
    }
    Consumer<JSONRPCMessage> handler = this.inbound;
    if (handler == null) {
      logger.warn("Dropping an inbound message: connect() has not been called");
      return;
    }
    JSONRPCMessage message;
    try {
      message = this.codec.decode(json);
    }
    catch (Exception e) {
      notifyException(new McpTransportException("Could not decode an inbound JSON-RPC message", e));
      return;
    }
    Contexts.run(this.context, () -> handler.accept(message));
  }

  private void notifyException(Throwable cause) {
    Consumer<Throwable> handler = this.exceptionHandler;
    if (handler != null) {
      Contexts.run(this.context, () -> handler.accept(cause));
    }
    else {
      logger.warn("Transport failure on {}", this.endpointUri, cause);
    }
  }

  private static String resolve(String baseUrl, String endpoint) {
    String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    if (endpoint == null || endpoint.isEmpty()) {
      return base;
    }
    return endpoint.startsWith("/") ? base + endpoint : base + "/" + endpoint;
  }

  // --- builder -------------------------------------------------------------

  /** Fluent configuration, mirroring {@code VertxMcpServer.Builder}. */
  public static final class Builder {

    private final Vertx vertx;

    private String baseUrl = "http://localhost:8080";

    private String endpoint = "/mcp";

    private HttpClient httpClient;

    private HttpClientOptions httpClientOptions = new HttpClientOptions();

    private final Map<String, String> headers = new LinkedHashMap<>();

    private Function<HttpClientRequest, HttpClientRequest> requestCustomizer;

    private Duration requestTimeout = Duration.ofSeconds(30);

    private boolean openListeningStream = true;

    private int maxReconnectAttempts = 3;

    private Duration reconnectDelay = Duration.ofSeconds(1);

    private Duration maxReconnectDelay = Duration.ofSeconds(30);

    private boolean terminateSessionOnClose = true;

    private List<String> protocolVersions = new ArrayList<>(
        List.of(ProtocolVersions.MCP_2025_03_26, ProtocolVersions.MCP_2025_06_18));

    private JsonRpcCodec codec;

    private Builder(Vertx vertx) {
      this.vertx = vertx;
    }

    /** Scheme, host and port, plus any path prefix; the endpoint is appended to it. */
    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    /** Path of the single MCP endpoint. Defaults to {@code /mcp}. */
    public Builder endpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /** Supplies the client to use. The transport will not close a client it did not create. */
    public Builder httpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public Builder httpClientOptions(HttpClientOptions options) {
      this.httpClientOptions = options;
      return this;
    }

    /** Adds a header sent on every request. */
    public Builder header(String name, String value) {
      this.headers.put(name, value);
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    /**
     * Last hook before a request is sent -- the place to attach credentials that have to
     * be recomputed per request, such as a refreshed bearer token.
     *
     * <p>
     * The request handed over is a vertx-core {@link HttpClientRequest}, not the Web
     * Client's {@code HttpRequest}: this transport talks to {@link HttpClient} directly so
     * that it can act on the response head while the body is still streaming. The Web
     * Client conveniences have plain equivalents -- {@code bearerTokenAuthentication(t)}
     * becomes {@code putHeader("Authorization", "Bearer " + t)}.
     */
    public Builder requestCustomizer(Function<HttpClientRequest, HttpClientRequest> requestCustomizer) {
      this.requestCustomizer = requestCustomizer;
      return this;
    }

    /**
     * How long a request may stall with no bytes moving before it is abandoned. It is an
     * idle deadline, not a total one, so a long streaming tool call is not cut short.
     */
    public Builder requestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    /** Whether to open the standalone server-to-client {@code GET} stream. Default true. */
    public Builder openListeningStream(boolean openListeningStream) {
      this.openListeningStream = openListeningStream;
      return this;
    }

    /** How many times to retry the standalone stream before giving up. Default 3. */
    public Builder maxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
      return this;
    }

    /** First backoff step; doubles per attempt, capped by {@link #maxReconnectDelay}. */
    public Builder reconnectDelay(Duration reconnectDelay) {
      this.reconnectDelay = reconnectDelay;
      return this;
    }

    public Builder maxReconnectDelay(Duration maxReconnectDelay) {
      this.maxReconnectDelay = maxReconnectDelay;
      return this;
    }

    /**
     * Whether {@link StreamableHttpClientTransport#close()} sends {@code DELETE} to end
     * the session server-side. Default true.
     */
    public Builder terminateSessionOnClose(boolean terminateSessionOnClose) {
      this.terminateSessionOnClose = terminateSessionOnClose;
      return this;
    }

    /** Protocol versions to offer at initialize, newest last. */
    public Builder protocolVersions(List<String> protocolVersions) {
      this.protocolVersions = new ArrayList<>(protocolVersions);
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    public StreamableHttpClientTransport build() {
      return new StreamableHttpClientTransport(this);
    }
  }
}
