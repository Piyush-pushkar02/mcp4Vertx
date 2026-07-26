package io.vertx.mcp.client.transport;

import java.net.URI;
import java.time.Duration;
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
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
 * The legacy HTTP+SSE client transport (protocol 2024-11-05), for servers that predate
 * streamable HTTP. Prefer {@link StreamableHttpClientTransport} for anything newer.
 *
 * <p>
 * Two channels instead of one. The client opens a long-lived {@code GET /sse} and the
 * server's very first event, {@code event: endpoint}, names the URL to POST to -- which is
 * why {@link #connect} does not complete until that event lands: sending anything before
 * it would have nowhere to go. Every server-to-client message then arrives on the SSE
 * stream, and every client-to-server message is a separate POST answered with
 * {@code 202 Accepted} and an empty body.
 */
public final class SseClientTransport implements McpClientTransport, McpProtocolVersionAware {

  private static final Logger logger = LoggerFactory.getLogger(SseClientTransport.class);

  private static final String ENDPOINT_EVENT = "endpoint";

  private static final String MESSAGE_EVENT = "message";

  private final Context context;

  private final HttpClient httpClient;

  private final boolean ownsHttpClient;

  private final String baseUrl;

  private final String sseUri;

  private final Map<String, String> headers;

  private final Function<HttpClientRequest, HttpClientRequest> requestCustomizer;

  private final Duration requestTimeout;

  private final JsonRpcCodec codec;

  private Consumer<JSONRPCMessage> inbound;

  private Consumer<Throwable> exceptionHandler;

  private Promise<Void> connected;

  private volatile String messageUri;

  private volatile String negotiatedProtocolVersion;

  private HttpClientResponse streamResponse;

  private boolean closed;

  private SseClientTransport(Builder builder) {
    this.context = Contexts.current(builder.vertx);
    this.ownsHttpClient = builder.httpClient == null;
    this.httpClient = this.ownsHttpClient ? builder.vertx.createHttpClient(builder.httpClientOptions)
        : builder.httpClient;
    this.baseUrl = builder.baseUrl.endsWith("/") ? builder.baseUrl.substring(0, builder.baseUrl.length() - 1)
        : builder.baseUrl;
    this.sseUri = this.baseUrl + (builder.sseEndpoint.startsWith("/") ? builder.sseEndpoint
        : "/" + builder.sseEndpoint);
    this.headers = Map.copyOf(builder.headers);
    this.requestCustomizer = builder.requestCustomizer;
    this.requestTimeout = builder.requestTimeout;
    this.codec = builder.codec != null ? builder.codec : JsonRpcCodec.defaultCodec();
  }

  public static Builder builder(Vertx vertx) {
    return new Builder(vertx);
  }

  /** The POST URL the server advertised, or {@code null} before the endpoint event. */
  public String messageUri() {
    return this.messageUri;
  }

  @Override
  public List<String> protocolVersions() {
    return List.of(ProtocolVersions.MCP_2024_11_05);
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
      if (this.connected != null) {
        return this.connected.future();
      }
      this.inbound = inboundHandler;
      this.connected = Promise.promise();
      RequestOptions options = requestOptions(HttpMethod.GET, this.sseUri)
        .putHeader(HttpHeaders.ACCEPT, "text/event-stream");
      // No idle timeout: this stream is expected to sit quiet between server messages.
      this.httpClient.request(options)
        .compose(request -> customize(request).send())
        .onSuccess(this::onStreamHead)
        .onFailure(this::onStreamEnded);
      return this.connected.future();
    });
  }

  private void onStreamHead(HttpClientResponse response) {
    if (this.closed) {
      response.request().reset();
      return;
    }
    this.streamResponse = response;
    int status = response.statusCode();
    if (status < 200 || status >= 300 || !MediaTypes.isEventStream(response.getHeader(HttpHeaders.CONTENT_TYPE))) {
      // Whatever this is, it is not the stream the whole transport hangs off, and there is
      // nothing to wait for: report it exactly as a stream that ended would be reported.
      response.body();
      onStreamEnded(new McpTransportException("SSE stream " + this.sseUri + " ended with status " + status));
      return;
    }
    new SseResponseReader(this.context, response, this::onEvent).start()
      .onComplete(result -> onStreamEnded(result.succeeded()
          ? new McpTransportException("SSE stream " + this.sseUri + " ended with status " + status)
          : result.cause()));
  }

  /**
   * The stream is over, which is always a failure for this transport: it is the only way
   * the server can reach us. Before the endpoint event that means {@link #connect} never
   * had a chance; after it, the session is already live and this is an out-of-band
   * failure.
   */
  private void onStreamEnded(Throwable cause) {
    this.streamResponse = null;
    if (this.closed) {
      return;
    }
    if (!this.connected.tryFail(cause)) {
      notifyException(cause);
    }
  }

  private void onEvent(SseEvent event) {
    if (ENDPOINT_EVENT.equals(event.event())) {
      onEndpoint(event.data());
      return;
    }
    if (MESSAGE_EVENT.equals(event.event()) || event.event() == null) {
      deliver(event.data());
      return;
    }
    logger.debug("Ignoring unexpected SSE event '{}'", event.event());
  }

  private void onEndpoint(String data) {
    if (data == null || data.isBlank()) {
      this.connected.tryFail(new McpTransportException("Server sent an empty 'endpoint' event"));
      return;
    }
    try {
      // Servers usually send a path with a session query parameter; resolving against the
      // SSE URL handles both that and a fully qualified URL.
      this.messageUri = URI.create(this.sseUri).resolve(data.trim()).toString();
    }
    catch (RuntimeException e) {
      this.connected.tryFail(new McpTransportException("Server sent an unusable 'endpoint' event: " + data, e));
      return;
    }
    logger.debug("SSE message endpoint is {}", this.messageUri);
    Contexts.run(this.context, () -> this.connected.tryComplete());
  }

  // --- outbound ------------------------------------------------------------

  @Override
  public Future<Void> sendMessage(JSONRPCMessage message) {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.failedFuture(new IllegalStateException("Transport is closed"));
      }
      String target = this.messageUri;
      if (target == null) {
        return Future.failedFuture(
            new IllegalStateException("No message endpoint yet; connect() has not completed"));
      }
      RequestOptions options = requestOptions(HttpMethod.POST, target)
        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .setTimeout(this.requestTimeout.toMillis());
      return this.httpClient.request(options)
        .compose(request -> customize(request).send(this.codec.encodeToBuffer(message)))
        .compose(response -> {
          int status = response.statusCode();
          // The answer, if any, comes back on the SSE stream, never in this response body,
          // which is drained only so the pooled connection can serve the next message.
          return response.body().compose(body -> status >= 200 && status < 300 ? Future.<Void>succeededFuture()
              : Future.<Void>failedFuture(new McpTransportException(
                  "POST " + target + " answered " + status + " " + response.statusMessage())));
        });
    });
  }

  @Override
  public Future<Void> close() {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      HttpClientResponse stream = this.streamResponse;
      this.streamResponse = null;
      if (stream != null) {
        stream.request().reset();
      }
      if (this.connected != null) {
        this.connected.tryFail(new IllegalStateException("Transport closed before the endpoint event arrived"));
      }
      if (this.ownsHttpClient) {
        this.httpClient.close();
      }
      return Future.succeededFuture();
    });
  }

  // --- plumbing ------------------------------------------------------------

  /**
   * Options for one request, with everything this transport always sends already on them.
   * Headers go on the options rather than on the request so the customizer, which runs
   * last, is free to override any of them.
   */
  private RequestOptions requestOptions(HttpMethod method, String uri) {
    RequestOptions options = new RequestOptions().setMethod(method).setAbsoluteURI(uri);
    // The Web Client this transport used to be built on followed redirects by default;
    // keeping that on means a relocated endpoint still works.
    options.setFollowRedirects(true);
    this.headers.forEach(options::putHeader);
    String version = this.negotiatedProtocolVersion;
    if (version != null) {
      options.putHeader(HttpHeaders.PROTOCOL_VERSION, version);
    }
    return options;
  }

  private HttpClientRequest customize(HttpClientRequest request) {
    return this.requestCustomizer != null ? this.requestCustomizer.apply(request) : request;
  }

  private void deliver(String json) {
    if (json == null || json.isBlank()) {
      return;
    }
    Consumer<JSONRPCMessage> handler = this.inbound;
    if (handler == null) {
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
      logger.warn("Transport failure on {}", this.sseUri, cause);
    }
  }

  // --- builder -------------------------------------------------------------

  /** Fluent configuration, mirroring {@code VertxMcpServer.Builder}. */
  public static final class Builder {

    private final Vertx vertx;

    private String baseUrl = "http://localhost:8080";

    private String sseEndpoint = "/sse";

    private HttpClient httpClient;

    private HttpClientOptions httpClientOptions = new HttpClientOptions();

    private final Map<String, String> headers = new LinkedHashMap<>();

    private Function<HttpClientRequest, HttpClientRequest> requestCustomizer;

    private Duration requestTimeout = Duration.ofSeconds(30);

    private JsonRpcCodec codec;

    private Builder(Vertx vertx) {
      this.vertx = vertx;
    }

    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    /** Path of the SSE stream. Defaults to {@code /sse}. */
    public Builder sseEndpoint(String sseEndpoint) {
      this.sseEndpoint = sseEndpoint;
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

    public Builder header(String name, String value) {
      this.headers.put(name, value);
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    /**
     * Last hook before a request is sent, for per-request credentials.
     *
     * <p>
     * The request handed over is a vertx-core {@link HttpClientRequest}, not the Web
     * Client's {@code HttpRequest}: this transport talks to {@link HttpClient} directly so
     * that it can read the SSE stream from the response head onwards. The Web Client
     * conveniences have plain equivalents -- {@code bearerTokenAuthentication(t)} becomes
     * {@code putHeader("Authorization", "Bearer " + t)}.
     */
    public Builder requestCustomizer(Function<HttpClientRequest, HttpClientRequest> requestCustomizer) {
      this.requestCustomizer = requestCustomizer;
      return this;
    }

    /** Deadline for a message POST. The SSE stream itself is never timed out. */
    public Builder requestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    public SseClientTransport build() {
      return new SseClientTransport(this);
    }
  }
}
