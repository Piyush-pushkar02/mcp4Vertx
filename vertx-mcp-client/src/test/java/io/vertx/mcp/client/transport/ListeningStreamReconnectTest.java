package io.vertx.mcp.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

/**
 * The standalone {@code GET} stream's life cycle: it is opened once the session exists,
 * reconnected with {@code Last-Event-ID} when the server drops it, and abandoned without
 * complaint when the server says it does not offer one.
 */
@ExtendWith(VertxExtension.class)
class ListeningStreamReconnectTest {

  private static final String NOTIFICATION = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}";

  private Future<Integer> serve(Vertx vertx, Handler<HttpServerRequest> handler) {
    return vertx.createHttpServer().requestHandler(handler).listen(0, "localhost").map(HttpServer::actualPort);
  }

  private static JSONRPCRequest ping() {
    return new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_PING, "c-1", null);
  }

  private static void answerPost(HttpServerRequest request) {
    request.body().onSuccess(body -> request.response()
      .putHeader("Content-Type", "application/json")
      .putHeader("Mcp-Session-Id", "s-1")
      .end("{\"jsonrpc\":\"2.0\",\"id\":\"c-1\",\"result\":{}}"));
  }

  @Test
  void reopensTheStreamWithLastEventIdAfterTheServerDropsIt(Vertx vertx, VertxTestContext testContext)
      throws Exception {
    List<String> lastEventIds = new ArrayList<>();
    List<JSONRPCMessage> received = new ArrayList<>();
    VertxTestContext reconnected = new VertxTestContext();
    serve(vertx, request -> {
      if (request.method() != HttpMethod.GET) {
        answerPost(request);
        return;
      }
      lastEventIds.add(request.getHeader("Last-Event-ID"));
      HttpServerResponse response = request.response();
      response.putHeader("Content-Type", "text/event-stream").setChunked(true);
      if (lastEventIds.size() == 1) {
        // One event, then the stream dies under the client the way a dropped connection
        // would leave it.
        response.write("id: e1\ndata: " + NOTIFICATION + "\n\n").onSuccess(written -> response.end());
        return;
      }
      response.write("id: e2\ndata: " + NOTIFICATION + "\n\n").onSuccess(written -> reconnected.completeNow());
    }).compose(port -> {
      StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .openListeningStream(true)
        .reconnectDelay(java.time.Duration.ofMillis(10))
        .build();
      return transport.connect(received::add).compose(connected -> transport.sendMessage(ping()));
    }).onFailure(testContext::failNow);

    assertThat(reconnected.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    vertx.setTimer(200, timer -> testContext.verify(() -> {
      // The first attempt cannot resume anything; the second one picks up where it left.
      assertThat(lastEventIds).containsExactly(null, "e1");
      assertThat(received).hasSize(3);
      testContext.completeNow();
    }));
  }

  @Test
  void acceptsAServerThatDeclinesTheStreamWithoutRetrying(Vertx vertx, VertxTestContext testContext)
      throws Exception {
    // Atomic rather than a plain counter: the server and the transport run on different
    // event loops, and nothing here synchronises them.
    AtomicInteger getAttempts = new AtomicInteger();
    VertxTestContext declined = new VertxTestContext();
    serve(vertx, request -> {
      if (request.method() != HttpMethod.GET) {
        answerPost(request);
        return;
      }
      getAttempts.incrementAndGet();
      request.response().setStatusCode(405).end().onSuccess(sent -> declined.completeNow());
    }).compose(port -> {
      StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .openListeningStream(true)
        .reconnectDelay(java.time.Duration.ofMillis(10))
        .build();
      return transport.connect(message -> {
      }).compose(connected -> transport.sendMessage(ping()));
    }).onFailure(testContext::failNow);

    assertThat(declined.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    // Long enough that a retry ladder starting at 10ms would have run to exhaustion.
    vertx.setTimer(500, timer -> testContext.verify(() -> {
      assertThat(getAttempts.get()).isEqualTo(1);
      testContext.completeNow();
    }));
  }
}
