package io.vertx.mcp.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

/** The legacy two-channel transport: endpoint discovery, POST out, SSE in. */
@ExtendWith(VertxExtension.class)
class SseClientTransportTest {

  private Future<Integer> serve(Vertx vertx, Handler<HttpServerRequest> handler) {
    return vertx.createHttpServer().requestHandler(handler).listen(0, "localhost").map(HttpServer::actualPort);
  }

  @Test
  void waitsForTheEndpointEventThenPostsThere(Vertx vertx, VertxTestContext testContext) throws Exception {
    List<JSONRPCMessage> received = new ArrayList<>();
    List<String> postedPaths = new ArrayList<>();
    VertxTestContext posted = new VertxTestContext();
    serve(vertx, request -> {
      if (request.method() == HttpMethod.GET) {
        HttpServerResponse response = request.response();
        response.putHeader("Content-Type", "text/event-stream").setChunked(true);
        // Split across two writes so the transport cannot rely on one chunk per event.
        response.write("event: endpoint\ndata: /messa");
        response.write("ges?sessionId=abc\n\n");
        return;
      }
      postedPaths.add(request.uri());
      request.body().onSuccess(body -> {
        request.response().setStatusCode(202).end();
        // The answer travels back on the SSE stream, never in the POST response.
        posted.completeNow();
      });
    }).compose(port -> {
      SseClientTransport transport = SseClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .build();
      return transport.connect(received::add).compose(connected -> {
        assertThat(transport.messageUri()).endsWith("/messages?sessionId=abc");
        return transport.sendMessage(
            new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_PING, "c-1", null));
      });
    }).onFailure(testContext::failNow);

    assertThat(posted.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    testContext.verify(() -> {
      assertThat(postedPaths).containsExactly("/messages?sessionId=abc");
      testContext.completeNow();
    });
  }

  @Test
  void deliversMessagesArrivingOnTheStream(Vertx vertx, VertxTestContext testContext) throws Exception {
    List<JSONRPCMessage> received = new ArrayList<>();
    VertxTestContext delivered = new VertxTestContext();
    serve(vertx, request -> {
      HttpServerResponse response = request.response();
      response.putHeader("Content-Type", "text/event-stream").setChunked(true);
      response.write("event: endpoint\ndata: /messages\n\n");
      response.write("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"c-1\",\"result\":{}}\n\n");
    }).compose(port -> {
      SseClientTransport transport = SseClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .build();
      return transport.connect(message -> {
        received.add(message);
        delivered.completeNow();
      });
    }).onFailure(testContext::failNow);

    assertThat(delivered.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    testContext.verify(() -> {
      assertThat(received).hasSize(1);
      assertThat(received.get(0)).isInstanceOf(JSONRPCResponse.class);
      testContext.completeNow();
    });
  }

  @Test
  void failsConnectWhenTheStreamDiesBeforeTheEndpointEvent(Vertx vertx, VertxTestContext testContext) {
    serve(vertx, request -> request.response().setStatusCode(500).end()).compose(port -> SseClientTransport
      .builder(vertx)
      .baseUrl("http://localhost:" + port)
      .build()
      .connect(message -> {
      })).onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpTransportException.class);
        testContext.completeNow();
      })));
  }
}
