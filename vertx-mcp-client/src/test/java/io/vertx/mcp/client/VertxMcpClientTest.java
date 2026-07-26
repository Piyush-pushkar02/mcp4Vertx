package io.vertx.mcp.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mcp.spec.McpTransports.McpClientTransport;

/**
 * Covers the client's own logic -- handshake ordering, the initialize gate, cursor
 * walking and the server-to-client handlers -- against a transport that answers in
 * memory, so nothing here depends on HTTP framing.
 */
@ExtendWith(VertxExtension.class)
class VertxMcpClientTest {

  private static final McpSchema.ServerCapabilities SERVER_CAPABILITIES = new McpSchema.ServerCapabilities(null,
      null, new McpSchema.ServerCapabilities.LoggingCapabilities(), null, null,
      new McpSchema.ServerCapabilities.ToolCapabilities(true));

  /** A transport that answers requests from a supplied function and can push messages. */
  private static final class FakeTransport implements McpClientTransport {

    private final Function<JSONRPCRequest, Object> responder;

    private final List<JSONRPCMessage> sent = new ArrayList<>();

    private Consumer<JSONRPCMessage> inbound;

    /** Promises for responses a test is waiting on, keyed by the request id it pushed. */
    private final Map<Object, Promise<JSONRPCResponse>> awaited = new ConcurrentHashMap<>();

    FakeTransport(Function<JSONRPCRequest, Object> responder) {
      this.responder = responder;
    }

    @Override
    public Future<Void> connect(Consumer<JSONRPCMessage> inboundHandler) {
      this.inbound = inboundHandler;
      return Future.succeededFuture();
    }

    @Override
    public Future<Void> sendMessage(JSONRPCMessage message) {
      this.sent.add(message);
      if (message instanceof JSONRPCResponse response) {
        Promise<JSONRPCResponse> waiting = this.awaited.remove(response.id());
        if (waiting != null) {
          waiting.tryComplete(response);
        }
      }
      if (message instanceof JSONRPCRequest request) {
        Object result = this.responder.apply(request);
        if (result != null) {
          this.inbound.accept(new JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), result, null));
        }
      }
      return Future.succeededFuture();
    }

    void push(JSONRPCMessage message) {
      this.inbound.accept(message);
    }

    /**
     * Pushes a server-initiated request and completes when the client has answered it.
     *
     * <p>
     * Asserting straight after {@code push} would race: the session hops onto its own Vert.x
     * context to dispatch, so when a test drives it from the JUnit thread the handler has not
     * run yet by the time the next statement executes.
     */
    Future<JSONRPCResponse> ask(JSONRPCRequest request) {
      Promise<JSONRPCResponse> promise = Promise.promise();
      this.awaited.put(request.id(), promise);
      this.inbound.accept(request);
      return promise.future();
    }

    @Override
    public Future<Void> close() {
      return Future.succeededFuture();
    }
  }

  private static Object handshake(JSONRPCRequest request) {
    return McpSchema.METHOD_INITIALIZE.equals(request.method())
        ? new McpSchema.InitializeResult("2025-06-18", SERVER_CAPABILITIES,
            new McpSchema.Implementation("fake", "1.0.0"), null)
        : null;
  }

  private VertxMcpClient client(Vertx vertx, FakeTransport transport) {
    return VertxMcpClient.builder(vertx).transport(transport).clientInfo("test-client", "1.0.0").build();
  }

  @Test
  void refusesRequestsBeforeInitialize(Vertx vertx, VertxTestContext testContext) {
    client(vertx, new FakeTransport(VertxMcpClientTest::handshake)).listTools()
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(IllegalStateException.class).hasMessageContaining("not initialized");
        testContext.completeNow();
      })));
  }

  @Test
  void handshakeSendsInitializeThenTheInitializedNotification(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(VertxMcpClientTest::handshake);
    VertxMcpClient client = client(vertx, transport);
    client.initialize().onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(transport.sent).hasSize(2);
      assertThat(((JSONRPCRequest) transport.sent.get(0)).method()).isEqualTo(McpSchema.METHOD_INITIALIZE);
      assertThat(((JSONRPCNotification) transport.sent.get(1)).method())
        .isEqualTo(McpSchema.METHOD_NOTIFICATION_INITIALIZED);
      assertThat(client.serverCapabilities()).isEqualTo(SERVER_CAPABILITIES);
      assertThat(client.protocolVersion()).isEqualTo("2025-06-18");
      assertThat(result.serverInfo().name()).isEqualTo("fake");
      testContext.completeNow();
    })));
  }

  @Test
  void refusesAProtocolVersionItCannotSpeak(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(request -> new McpSchema.InitializeResult("1999-01-01",
        SERVER_CAPABILITIES, new McpSchema.Implementation("fake", "1.0.0"), null));
    client(vertx, transport).initialize().onComplete(testContext.failing(cause -> testContext.verify(() -> {
      assertThat(cause).isInstanceOf(IllegalStateException.class).hasMessageContaining("1999-01-01");
      testContext.completeNow();
    })));
  }

  @Test
  void walksCursorsToCollectEveryTool(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(request -> {
      if (McpSchema.METHOD_INITIALIZE.equals(request.method())) {
        return handshake(request);
      }
      String cursor = ((McpSchema.PaginatedRequest) request.params()).cursor();
      return cursor == null ? new McpSchema.ListToolsResult(List.of(tool("a"), tool("b")), "page-2")
          : new McpSchema.ListToolsResult(List.of(tool("c")), null);
    });
    VertxMcpClient client = client(vertx, transport);
    client.initialize()
      .compose(ready -> client.listAllTools())
      .onComplete(testContext.succeeding(tools -> testContext.verify(() -> {
        assertThat(tools).extracting(McpSchema.Tool::name).containsExactly("a", "b", "c");
        testContext.completeNow();
      })));
  }

  @Test
  void answersRootsListFromTheConfiguredRoots(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(VertxMcpClientTest::handshake);
    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(transport)
      .roots(List.of(new McpSchema.Root("file:///work", "work")))
      .build();
    client.initialize().onComplete(testContext.succeeding(ready -> {
      Future<JSONRPCResponse> answered = transport
        .ask(new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_ROOTS_LIST, "s-1", Map.of()));
      answered.onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertThat(response.id()).isEqualTo("s-1");
        assertThat(((McpSchema.ListRootsResult) response.result()).roots()).hasSize(1);
        testContext.completeNow();
      })));
    }));
  }

  @Test
  void reportsSamplingAsUnsupportedWhenNoHandlerIsConfigured(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(VertxMcpClientTest::handshake);
    VertxMcpClient client = client(vertx, transport);
    client.initialize().onComplete(testContext.succeeding(ready -> {
      Future<JSONRPCResponse> answered = transport
        .ask(new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_SAMPLING_CREATE_MESSAGE, "s-2", Map.of("messages", List.of(), "maxTokens", 10)));
      answered.onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertThat(response.error()).isNotNull();
        assertThat(response.error().code()).isEqualTo(McpSchema.ErrorCodes.METHOD_NOT_FOUND);
        assertThat(response.error().message()).contains("does not support sampling");
        assertThat(client.clientCapabilities().sampling()).isNull();
        testContext.completeNow();
      })));
    }));
  }

  @Test
  void delegatesSamplingToTheConfiguredHandler(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(VertxMcpClientTest::handshake);
    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(transport)
      .sampling(request -> Future.succeededFuture(new McpSchema.CreateMessageResult(McpSchema.Role.ASSISTANT,
          new McpSchema.TextContent("hi"), "test-model", McpSchema.CreateMessageResult.StopReason.END_TURN, null)))
      .build();
    assertThat(client.clientCapabilities().sampling()).isNotNull();
    client.initialize().onComplete(testContext.succeeding(ready -> {
      Future<JSONRPCResponse> answered = transport
        .ask(new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_SAMPLING_CREATE_MESSAGE, "s-3", Map.of("messages", List.of(), "maxTokens", 10)));
      answered.onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertThat(response.error()).isNull();
        assertThat(((McpSchema.CreateMessageResult) response.result()).model()).isEqualTo("test-model");
        testContext.completeNow();
      })));
    }));
  }

  @Test
  void routesServerNotificationsToTheirHandlers(Vertx vertx, VertxTestContext testContext) {
    FakeTransport transport = new FakeTransport(VertxMcpClientTest::handshake);
    List<String> observed = new CopyOnWriteArrayList<>();
    // Notifications produce nothing on the wire, so the only completion signal is the
    // handlers themselves firing. Waiting on the fourth one avoids asserting before the
    // session has dispatched them on its own context.
    Promise<Void> allSeen = Promise.promise();
    Consumer<String> record = event -> {
      observed.add(event);
      if (observed.size() == 4) {
        allSeen.tryComplete();
      }
    };
    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(transport)
      .progressHandler(progress -> record.accept("progress:" + progress.progress()))
      .loggingHandler(message -> record.accept("log:" + message.data()))
      .toolsChangeHandler(() -> record.accept("tools-changed"))
      .resourceUpdatedHandler(uri -> record.accept("updated:" + uri))
      .build();
    client.initialize().onComplete(testContext.succeeding(ready -> {
      transport.push(new JSONRPCNotification(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_NOTIFICATION_PROGRESS,
          Map.of("progressToken", "t", "progress", 0.25)));
      transport.push(new JSONRPCNotification(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_NOTIFICATION_MESSAGE,
          Map.of("level", "info", "data", "working")));
      transport.push(new JSONRPCNotification(McpSchema.JSONRPC_VERSION,
          McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, null));
      transport.push(new JSONRPCNotification(McpSchema.JSONRPC_VERSION,
          McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED, Map.of("uri", "file:///a")));
      allSeen.future().onComplete(testContext.succeeding(done -> testContext.verify(() -> {
        assertThat(observed).containsExactly("progress:0.25", "log:working", "tools-changed", "updated:file:///a");
        testContext.completeNow();
      })));
    }));
  }

  private static McpSchema.Tool tool(String name) {
    return McpSchema.Tool.builder().name(name).description(name).inputSchema(new McpSchema.JsonSchema("object",
        Map.of(), List.of(), null, null, null)).build();
  }

}
