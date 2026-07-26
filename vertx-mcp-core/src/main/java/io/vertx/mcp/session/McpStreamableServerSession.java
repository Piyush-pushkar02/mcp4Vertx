package io.vertx.mcp.session;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.McpTransportContext;
import io.vertx.mcp.internal.Contexts;
import io.vertx.mcp.spec.EventStore;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpHandlers;
import io.vertx.mcp.spec.McpPeerInfo;
import io.vertx.mcp.spec.McpSession;
import io.vertx.mcp.spec.McpTransports.McpStreamableServerTransport;

/**
 * A Streamable HTTP session (protocol 2025-03-26 and later).
 *
 * <p>
 * One session fans out over several concurrent streams:
 * <ul>
 * <li>a <b>listening stream</b> from {@code GET /mcp}, which carries unsolicited
 * server-to-client traffic;</li>
 * <li>a <b>response stream</b> per {@code POST /mcp} that contained a request -- anything
 * the handler emits (progress notifications, sampling requests) rides back on that same
 * SSE response, which is what the spec asks for.</li>
 * </ul>
 *
 * <p>
 * Unlike the upstream SDK -- whose {@code replay()} returns an empty {@code Flux}, i.e.
 * resumability is declared but not implemented -- this session records every outbound
 * message in an {@link EventStore} and can genuinely resume a dropped stream from
 * {@code Last-Event-ID}.
 */
public class McpStreamableServerSession extends AbstractMcpSession implements McpPeerInfo {

  private static final Logger logger = LoggerFactory.getLogger(McpStreamableServerSession.class);

  private final String id;

  private final EventStore eventStore;

  /** Set on GET /mcp. Null until the client opens one; notifications are dropped then. */
  private SessionStream listeningStream;

  /** Which stream issued a given server-to-client request, so we can clean up on close. */
  private final Map<Object, SessionStream> requestIdToStream = new HashMap<>();

  private final Set<SessionStream> openStreams = new HashSet<>();

  private volatile McpSchema.ClientCapabilities clientCapabilities;

  private volatile McpSchema.Implementation clientInfo;

  private volatile String protocolVersion;

  public McpStreamableServerSession(Vertx vertx, String id, JsonRpcCodec codec, Duration requestTimeout,
      Map<String, McpHandlers.RequestHandler> requestHandlers,
      Map<String, McpHandlers.NotificationHandler> notificationHandlers, EventStore eventStore) {
    super(vertx, codec, requestTimeout, requestHandlers, notificationHandlers);
    this.id = id;
    this.eventStore = eventStore;
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
  public McpSchema.ClientCapabilities clientCapabilities() {
    return this.clientCapabilities;
  }

  @Override
  public McpSchema.Implementation clientInfo() {
    return this.clientInfo;
  }

  public String protocolVersion() {
    return this.protocolVersion;
  }

  public void clientInfo(McpSchema.ClientCapabilities capabilities, McpSchema.Implementation info,
      String protocolVersion) {
    this.clientCapabilities = capabilities;
    this.clientInfo = info;
    this.protocolVersion = protocolVersion;
  }

  /**
   * Unsolicited server-to-client traffic goes out on the listening stream. If the client
   * has not opened one, the write fails rather than silently vanishing -- callers that
   * treat notifications as best-effort should ignore the failure explicitly.
   */
  @Override
  protected Future<Void> write(JSONRPCMessage message) {
    SessionStream stream = this.listeningStream;
    if (stream == null) {
      return Future.failedFuture(
          new IllegalStateException("Session " + this.id + " has no listening stream (client has not opened GET /mcp)"));
    }
    return stream.write(message);
  }

  @Override
  public <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType) {
    return Contexts.on(this.context, () -> {
      SessionStream stream = this.listeningStream;
      if (stream == null) {
        return Future.failedFuture(new IllegalStateException(
            "Session " + this.id + " has no listening stream to receive the response to '" + method + "'"));
      }
      return stream.sendRequest(method, params, resultType);
    });
  }

  // --- stream lifecycle ----------------------------------------------------

  /** Binds the {@code GET /mcp} stream. Replaces any previous one. */
  public Future<SessionStream> listeningStream(McpStreamableServerTransport transport) {
    return Contexts.on(this.context, () -> {
      SessionStream stream = new SessionStream(transport, this.id + "-listening");
      this.listeningStream = stream;
      this.openStreams.add(stream);
      return Future.succeededFuture(stream);
    });
  }

  /**
   * Resumes a dropped stream: replays everything recorded after {@code lastEventId} onto
   * the new transport, then binds it as the listening stream.
   */
  public Future<SessionStream> resumeStream(McpStreamableServerTransport transport, String lastEventId) {
    return Contexts.on(this.context,
        () -> this.eventStore.replayAfter(lastEventId, (eventId, message) -> transport.sendMessage(message, eventId))
          .compose(streamId -> {
            SessionStream stream = new SessionStream(transport, streamId);
            this.listeningStream = stream;
            this.openStreams.add(stream);
            return Future.succeededFuture(stream);
          }));
  }

  /**
   * Handles a request that arrived on a POST, writing the result -- and anything the
   * handler emitted along the way -- to {@code transport}, then closing it.
   */
  public Future<Void> responseStream(JSONRPCRequest request, McpStreamableServerTransport transport,
      McpTransportContext transportContext) {
    return Contexts.on(this.context, () -> {
      SessionStream stream = new SessionStream(transport, this.id + "-" + request.id());
      this.openStreams.add(stream);
      return invokeRequestHandler(request, transportContext, stream)
        .compose(response -> response == null ? Future.succeededFuture() : stream.write(response))
        .eventually(() -> closeStream(stream));
    });
  }

  public Future<Void> handleNotification(JSONRPCNotification notification, McpTransportContext transportContext) {
    return Contexts.on(this.context, () -> invokeNotificationHandler(notification, transportContext));
  }

  /** Correlates a client's answer to a server-initiated request. */
  public Future<Void> handleClientResponse(JSONRPCResponse response) {
    return Contexts.on(this.context, () -> {
      this.requestIdToStream.remove(response.id());
      handleResponse(response);
      return Future.succeededFuture();
    });
  }

  private Future<Void> closeStream(SessionStream stream) {
    return Contexts.on(this.context, () -> {
      this.openStreams.remove(stream);
      if (this.listeningStream == stream) {
        this.listeningStream = null;
      }
      List<Object> orphaned = new ArrayList<>();
      this.requestIdToStream.forEach((requestId, owner) -> {
        if (owner == stream) {
          orphaned.add(requestId);
        }
      });
      orphaned.forEach(requestId -> {
        this.requestIdToStream.remove(requestId);
        failPending(requestId, new IllegalStateException("Stream closed before the client answered"));
      });
      return stream.transport.close().recover(err -> {
        logger.debug("Ignoring error closing stream for session {}", this.id, err);
        return Future.succeededFuture();
      });
    });
  }

  @Override
  protected Future<Void> doClose() {
    List<SessionStream> streams = new ArrayList<>(this.openStreams);
    this.openStreams.clear();
    this.listeningStream = null;
    this.requestIdToStream.clear();
    List<Future<?>> closes = new ArrayList<>();
    for (SessionStream stream : streams) {
      closes.add(stream.transport.close().recover(err -> Future.succeededFuture()));
    }
    return Future.all(closes).mapEmpty();
  }

  /**
   * A single SSE response bound to this session. It is a {@link McpSession} in its own
   * right so a request handler can send notifications and reverse-requests that land on
   * the correct HTTP response, while correlation state stays on the parent session.
   */
  public final class SessionStream implements McpSession, McpPeerInfo {

    @Override
    public McpSchema.ClientCapabilities clientCapabilities() {
      return McpStreamableServerSession.this.clientCapabilities();
    }

    @Override
    public McpSchema.Implementation clientInfo() {
      return McpStreamableServerSession.this.clientInfo();
    }


    private final McpStreamableServerTransport transport;

    private final String streamId;

    SessionStream(McpStreamableServerTransport transport, String streamId) {
      this.transport = transport;
      this.streamId = streamId;
    }

    public String streamId() {
      return this.streamId;
    }

    /** Records the message for replay, then writes it tagged with its event id. */
    Future<Void> write(JSONRPCMessage message) {
      return McpStreamableServerSession.this.eventStore.append(this.streamId, message)
        .compose(eventId -> this.transport.sendMessage(message, eventId));
    }

    @Override
    public <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType) {
      return McpStreamableServerSession.this.sendRequest(method, params, resultType, this::write,
          requestId -> McpStreamableServerSession.this.requestIdToStream.put(requestId, this));
    }

    @Override
    public Future<Void> sendNotification(String method, Object params) {
      return Contexts.on(McpStreamableServerSession.this.context,
          () -> write(new JSONRPCNotification(McpSchema.JSONRPC_VERSION, method, params)));
    }

    @Override
    public Future<Void> close() {
      return closeStream(this);
    }

    @Override
    public boolean isLoggingLevelEnabled(McpSchema.LoggingLevel level) {
      return McpStreamableServerSession.this.isLoggingLevelEnabled(level);
    }

    @Override
    public void setMinLoggingLevel(McpSchema.LoggingLevel level) {
      McpStreamableServerSession.this.setMinLoggingLevel(level);
    }
  }

  /** Generates a session id. Overridable for deterministic tests. */
  public static String newSessionId() {
    return UUID.randomUUID().toString();
  }
}
