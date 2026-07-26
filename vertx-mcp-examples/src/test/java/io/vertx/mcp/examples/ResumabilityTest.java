package io.vertx.mcp.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.RequestOptions;
import io.vertx.ext.web.Router;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mcp.server.VertxMcpServer;
import io.vertx.mcp.server.transport.StreamableHttpServerTransport;
import io.vertx.mcp.spec.InMemoryEventStore;
import io.vertx.mcp.spec.JsonRpcCodec;

/**
 * The property this class establishes: with an {@link InMemoryEventStore} configured, a
 * client whose standalone {@code GET} stream is severed can reconnect with
 * {@code Last-Event-ID} and receive exactly the messages recorded after that event --
 * every one of them, each exactly once, in order, and nothing it had already seen.
 *
 * <p>
 * Driven with a raw {@link HttpClient} rather than the client transport, because the
 * point is to control precisely when the socket dies and what the reconnect asks for.
 *
 * <p>
 * Note what this can and cannot cover today. The replay window is the set of messages the
 * server actually recorded, and a message is only recorded once it has a stream to go to:
 * {@code McpStreamableServerSession.write} rejects an unsolicited notification when no
 * listening stream is bound, <em>before</em> reaching the event store. So a notification
 * produced while the client is between connections is dropped outright rather than queued
 * for replay, and no test can assert otherwise until that changes. What is asserted here
 * is the property that actually holds: everything recorded after the anchor event is
 * replayed, in order, exactly once.
 *
 * <p>
 * The one ordering subtlety worth naming: a {@code GET} handler binds its listening
 * stream by queueing work on the session's Vert.x context, so nothing on the wire says
 * when the binding is done. The test therefore sends a {@code ping} on the same session
 * after the stream's response head arrives -- that request is queued on the same context
 * behind the binding, so its answer proves the binding happened. No sleeping.
 */
@ExtendWith(VertxExtension.class)
class ResumabilityTest {

  private final JsonRpcCodec codec = JsonRpcCodec.defaultCodec();

  private VertxMcpServer server;

  private HttpServer httpServer;

  private int port;

  @BeforeEach
  void startServer(Vertx vertx, VertxTestContext testContext) {
    this.server = VertxMcpServer.builder(vertx)
      .serverInfo("resumable", "1.0.0")
      .eventStore(new InMemoryEventStore())
      .tool(McpSchema.Tool.builder().name("noop").description("does nothing").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder().build()))
      .build();
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, this.server).router());
    vertx.createHttpServer().requestHandler(router).listen(0).onComplete(testContext.succeeding(http -> {
      this.httpServer = http;
      this.port = http.actualPort();
      testContext.completeNow();
    }));
  }

  @AfterEach
  void stopServer(VertxTestContext testContext) {
    this.httpServer.close().onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void aSeveredListeningStreamResumesFromLastEventIdWithoutLossOrDuplication(Vertx vertx,
      VertxTestContext testContext) {
    HttpClient http = vertx.createHttpClient();
    SseCollector live = new SseCollector();
    SseCollector replay = new SseCollector();
    AtomicReference<String> session = new AtomicReference<>();
    AtomicReference<HttpClientResponse> liveStream = new AtomicReference<>();

    initialize(http)
      .compose(sessionId -> {
        session.set(sessionId);
        return post(http, sessionId, notification(McpSchema.METHOD_NOTIFICATION_INITIALIZED));
      })
      .compose(v -> openListeningStream(http, session.get(), null, live))
      .compose(response -> {
        liveStream.set(response);
        // Round-trips through the session context, so the listening stream is bound by
        // the time this completes.
        return post(http, session.get(), request("ping-1", McpSchema.METHOD_PING));
      })
      // Three unsolicited notifications, each awaited so they cannot race each other.
      .compose(v -> this.server.notifyToolsListChanged())
      .compose(v -> this.server.notifyPromptsListChanged())
      .compose(v -> this.server.notifyResourceUpdated("test://marker"))
      .compose(v -> live.awaitFrames(3))
      .compose(frames -> {
        testContext.verify(() -> {
          assertThat(frames).extracting(SseFrame::method).containsExactly(
              McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED,
              McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED,
              McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED);
          assertThat(frames).allSatisfy(frame -> assertThat(frame.id()).isNotBlank());
        });
        // Kill the socket underneath the client, the way a dropped connection would.
        return closeConnection(liveStream.get()).map(frames);
      })
      .compose(frames -> openListeningStream(http, session.get(), frames.get(0).id(), replay).map(frames))
      .compose(frames -> replay.awaitFrames(2).map(replayed -> Map.entry(frames, replayed)))
      .onComplete(testContext.succeeding(both -> testContext.verify(() -> {
        List<SseFrame> original = both.getKey();
        List<SseFrame> replayed = both.getValue();

        // Everything after the anchor, in order, once each.
        assertThat(replayed).extracting(SseFrame::id)
          .containsExactly(original.get(1).id(), original.get(2).id());
        assertThat(replayed).extracting(SseFrame::method).containsExactly(
            McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED,
            McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED);
        // The anchor event itself is not re-sent, so nothing is delivered twice.
        assertThat(replayed).extracting(SseFrame::id).doesNotContain(original.get(0).id());
        // And nothing appeared that the live stream never recorded: the replay stops at
        // the end of the store rather than inventing or losing an event.
        assertThat(replay.frames()).hasSize(2);
        testContext.completeNow();
      })));
  }

  @Test
  void anUnknownLastEventIdStillYieldsAWorkingStream(Vertx vertx, VertxTestContext testContext) {
    HttpClient http = vertx.createHttpClient();
    SseCollector stream = new SseCollector();
    AtomicReference<String> session = new AtomicReference<>();

    initialize(http)
      .compose(sessionId -> {
        session.set(sessionId);
        return post(http, sessionId, notification(McpSchema.METHOD_NOTIFICATION_INITIALIZED));
      })
      // Resumption is best-effort: a stale id costs the client its replay, not its stream.
      .compose(v -> openListeningStream(http, session.get(), "no-such-event-id", stream))
      .compose(response -> post(http, session.get(), request("ping-1", McpSchema.METHOD_PING)))
      .compose(v -> this.server.notifyToolsListChanged())
      .compose(v -> stream.awaitFrames(1))
      .onComplete(testContext.succeeding(frames -> testContext.verify(() -> {
        assertThat(frames.get(0).method()).isEqualTo(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED);
        testContext.completeNow();
      })));
  }

  // --- raw HTTP plumbing ---------------------------------------------------

  private Future<String> initialize(HttpClient http) {
    McpSchema.InitializeRequest params = new McpSchema.InitializeRequest(ProtocolVersions.MCP_2025_06_18,
        new McpSchema.ClientCapabilities(null, null, null, null),
        new McpSchema.Implementation("raw-client", "1.0.0"));
    Buffer body = this.codec.encodeToBuffer(new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION,
        McpSchema.METHOD_INITIALIZE, "init-1", params));
    return http
      .request(new RequestOptions().setMethod(HttpMethod.POST)
        .setPort(this.port)
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader("Accept", "application/json, text/event-stream")
        .putHeader("Content-Type", "application/json"))
      .compose(request -> request.send(body))
      .compose(response -> response.body().map(ignored -> response.getHeader(HttpHeaders.MCP_SESSION_ID)));
  }

  /** POSTs one message and drains whatever comes back, so the connection is reusable. */
  private Future<Void> post(HttpClient http, String sessionId, Object message) {
    return http
      .request(new RequestOptions().setMethod(HttpMethod.POST)
        .setPort(this.port)
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader("Accept", "application/json, text/event-stream")
        .putHeader("Content-Type", "application/json")
        .putHeader(HttpHeaders.MCP_SESSION_ID, sessionId))
      .compose(request -> request.send(this.codec.encodeToBuffer(message)))
      .compose(response -> response.body())
      .mapEmpty();
  }

  private Future<HttpClientResponse> openListeningStream(HttpClient http, String sessionId, String lastEventId,
      SseCollector collector) {
    RequestOptions options = new RequestOptions().setMethod(HttpMethod.GET)
      .setPort(this.port)
      .setHost("localhost")
      .setURI("/mcp")
      .putHeader("Accept", "text/event-stream")
      .putHeader(HttpHeaders.MCP_SESSION_ID, sessionId);
    if (lastEventId != null) {
      options.putHeader(HttpHeaders.LAST_EVENT_ID, lastEventId);
    }
    return http.request(options).compose(request -> request.send()).map(response -> {
      response.handler(collector::feed);
      return response;
    });
  }

  private static Future<Void> closeConnection(HttpClientResponse response) {
    Promise<Void> closed = Promise.promise();
    response.request().connection().closeHandler(v -> closed.tryComplete());
    response.request().connection().close();
    return closed.future();
  }

  private static McpSchema.JSONRPCRequest request(String id, String method) {
    return new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION, method, id, Map.of());
  }

  private static McpSchema.JSONRPCNotification notification(String method) {
    return new McpSchema.JSONRPCNotification(McpSchema.JSONRPC_VERSION, method, Map.of());
  }

  // --- a minimal SSE reader ------------------------------------------------

  /** One {@code id:}/{@code event:}/{@code data:} frame, with the JSON-RPC method in it. */
  private record SseFrame(String id, String event, String data, String method) {
  }

  /**
   * Accumulates SSE frames off a raw response and lets a test await the n-th one. Comment
   * lines (the transport's {@code : connected} and keep-alives) are skipped.
   */
  private static final class SseCollector {

    private final StringBuilder pending = new StringBuilder();

    private final List<SseFrame> frames = new ArrayList<>();

    private int awaited = -1;

    private Promise<List<SseFrame>> promise;

    synchronized List<SseFrame> frames() {
      return List.copyOf(this.frames);
    }

    synchronized Future<List<SseFrame>> awaitFrames(int count) {
      if (this.frames.size() >= count) {
        return Future.succeededFuture(List.copyOf(this.frames.subList(0, count)));
      }
      this.awaited = count;
      this.promise = Promise.promise();
      return this.promise.future();
    }

    synchronized void feed(Buffer buffer) {
      this.pending.append(buffer.toString(java.nio.charset.StandardCharsets.UTF_8));
      int split;
      while ((split = this.pending.indexOf("\n\n")) >= 0) {
        String block = this.pending.substring(0, split);
        this.pending.delete(0, split + 2);
        SseFrame frame = parse(block);
        if (frame != null) {
          this.frames.add(frame);
        }
      }
      if (this.promise != null && this.frames.size() >= this.awaited) {
        Promise<List<SseFrame>> done = this.promise;
        this.promise = null;
        done.complete(List.copyOf(this.frames.subList(0, this.awaited)));
      }
    }

    private SseFrame parse(String block) {
      String id = null;
      String event = null;
      StringBuilder data = new StringBuilder();
      for (String line : block.split("\n")) {
        if (line.isEmpty() || line.startsWith(":")) {
          continue;
        }
        if (line.startsWith("id:")) {
          id = line.substring(3).trim();
        }
        else if (line.startsWith("event:")) {
          event = line.substring(6).trim();
        }
        else if (line.startsWith("data:")) {
          data.append(line.substring(5).trim());
        }
      }
      if (data.length() == 0) {
        return null;
      }
      String json = data.toString();
      String method = null;
      int at = json.indexOf("\"method\"");
      if (at >= 0) {
        int start = json.indexOf('"', json.indexOf(':', at) + 1);
        int end = json.indexOf('"', start + 1);
        method = json.substring(start + 1, end);
      }
      return new SseFrame(id, event, json, method);
    }
  }
}
