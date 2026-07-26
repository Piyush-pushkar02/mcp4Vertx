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
package io.github.mcp4vertx.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
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

/**
 * Exercises the transport against a real Vert.x HTTP server, because the behaviour that
 * matters -- messages surfacing while a response is still open -- cannot be observed
 * through a mock that hands back a finished body.
 */
@ExtendWith(VertxExtension.class)
class StreamableHttpClientTransportTest {

  private static final String PROGRESS = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\","
      + "\"params\":{\"progressToken\":\"t1\",\"progress\":0.5,\"total\":1.0}}";

  private static final String RESULT = "{\"jsonrpc\":\"2.0\",\"id\":\"c-1\",\"result\":{\"content\":[]}}";

  private Future<Integer> serve(Vertx vertx, Handler<HttpServerRequest> handler) {
    return vertx.createHttpServer().requestHandler(handler).listen(0, "localhost").map(HttpServer::actualPort);
  }

  private static HttpServerResponse sseResponse(HttpServerRequest request, String sessionId) {
    HttpServerResponse response = request.response();
    response.putHeader("Content-Type", "text/event-stream").setChunked(true);
    if (sessionId != null) {
      response.putHeader("Mcp-Session-Id", sessionId);
    }
    return response;
  }

  private static JSONRPCRequest ping() {
    return new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_PING, "c-1", null);
  }

  @Test
  void deliversEverySseMessageAsItArrivesAndCapturesTheSessionId(Vertx vertx, VertxTestContext testContext) {
    List<JSONRPCMessage> received = new ArrayList<>();
    List<String> sessionIdAtDelivery = new ArrayList<>();
    serve(vertx, request -> {
      if (request.method() != HttpMethod.POST) {
        request.response().setStatusCode(405).end();
        return;
      }
      request.body().onSuccess(body -> {
        HttpServerResponse response = sseResponse(request, "session-42");
        response.write("id: 1\nevent: message\ndata: " + PROGRESS + "\n\n");
        // Deliberately later, and split mid-event, so a transport that waited for the
        // whole body or mishandled a boundary would be caught here.
        vertx.setTimer(30, timer -> {
          response.write("id: 2\ndata: " + RESULT.substring(0, 20));
          response.write(RESULT.substring(20) + "\n\n");
          response.end();
        });
      });
    }).compose(port -> {
      StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .openListeningStream(false)
        .build();
      return transport.connect(message -> {
        received.add(message);
        sessionIdAtDelivery.add(transport.sessionId());
      }).compose(connected -> transport.sendMessage(ping())).map(transport);
    }).onComplete(testContext.succeeding(transport -> testContext.verify(() -> {
      assertThat(received).hasSize(2);
      assertThat(received.get(0)).isInstanceOf(JSONRPCNotification.class);
      assertThat(((JSONRPCNotification) received.get(0)).method()).isEqualTo("notifications/progress");
      assertThat(received.get(1)).isInstanceOf(JSONRPCResponse.class);
      assertThat(transport.sessionId()).isEqualTo("session-42");
      // The head interceptor must have captured the session id before the first message
      // was handed over, not when the response finally completed.
      assertThat(sessionIdAtDelivery).containsExactly("session-42", "session-42");
      testContext.completeNow();
    })));
  }

  @Test
  void decodesASingleJsonResponse(Vertx vertx, VertxTestContext testContext) {
    List<JSONRPCMessage> received = new ArrayList<>();
    serve(vertx, request -> request.body().onSuccess(body -> request.response()
      .putHeader("Content-Type", "application/json")
      .putHeader("Mcp-Session-Id", "json-1")
      .end(RESULT))).compose(port -> {
        StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
          .baseUrl("http://localhost:" + port)
          .openListeningStream(false)
          .build();
        return transport.connect(received::add).compose(connected -> transport.sendMessage(ping())).map(transport);
      })
      .onComplete(testContext.succeeding(transport -> testContext.verify(() -> {
        assertThat(received).hasSize(1);
        assertThat(received.get(0)).isInstanceOf(JSONRPCResponse.class);
        assertThat(transport.sessionId()).isEqualTo("json-1");
        testContext.completeNow();
      })));
  }

  @Test
  void treatsAnAcceptedResponseAsNothingToDeliver(Vertx vertx, VertxTestContext testContext) {
    List<JSONRPCMessage> received = new ArrayList<>();
    serve(vertx, request -> request.body().onSuccess(body -> request.response().setStatusCode(202).end()))
      .compose(port -> {
        StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
          .baseUrl("http://localhost:" + port)
          .openListeningStream(false)
          .build();
        return transport.connect(received::add)
          .compose(connected -> transport
            .sendMessage(new JSONRPCNotification(McpSchema.JSONRPC_VERSION, "notifications/initialized", null)));
      })
      .onComplete(testContext.succeeding(sent -> testContext.verify(() -> {
        assertThat(received).isEmpty();
        testContext.completeNow();
      })));
  }

  @Test
  void reportsAnExpiredSessionDistinctly(Vertx vertx, VertxTestContext testContext) {
    serve(vertx, request -> request.body().onSuccess(body -> request.response().setStatusCode(404).end()))
      .compose(port -> {
        StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
          .baseUrl("http://localhost:" + port)
          .openListeningStream(false)
          .build();
        return transport.connect(message -> {
        }).compose(connected -> transport.sendMessage(ping()));
      })
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpSessionNotFoundException.class);
        testContext.completeNow();
      })));
  }

  @Test
  void opensTheStandaloneStreamOnceTheSessionExists(Vertx vertx, VertxTestContext testContext) throws Exception {
    List<JSONRPCMessage> received = new ArrayList<>();
    List<String> listeningSessionIds = new ArrayList<>();
    VertxTestContext pushed = new VertxTestContext();
    serve(vertx, request -> {
      if (request.method() == HttpMethod.GET) {
        listeningSessionIds.add(request.getHeader("Mcp-Session-Id"));
        HttpServerResponse response = sseResponse(request, null);
        response.write("data: " + PROGRESS + "\n\n").onSuccess(written -> pushed.completeNow());
        return;
      }
      request.body().onSuccess(body -> request.response()
        .putHeader("Content-Type", "application/json")
        .putHeader("Mcp-Session-Id", "listen-7")
        .end("{\"jsonrpc\":\"2.0\",\"id\":\"c-1\",\"result\":{}}"));
    }).compose(port -> {
      StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .openListeningStream(true)
        .build();
      return transport.connect(received::add).compose(connected -> transport.sendMessage(ping()));
    }).onFailure(testContext::failNow);

    assertThat(pushed.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    // The push arrives asynchronously after the write completes server-side.
    vertx.setTimer(200, timer -> testContext.verify(() -> {
      assertThat(listeningSessionIds).containsExactly("listen-7");
      assertThat(received).hasSize(2);
      testContext.completeNow();
    }));
  }
}
