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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.RequestOptions;
import io.vertx.ext.web.Router;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.spec.JsonRpcCodec;

/**
 * End-to-end check of the streamable transport's initialize handshake over a real socket:
 * boot an {@link HttpServer} on the transport's router, POST an {@code initialize}
 * request, and assert the spec-mandated response shape (200, plain JSON, a
 * {@code Mcp-Session-Id} header and a well-formed {@code InitializeResult}).
 */
@ExtendWith(VertxExtension.class)
class StreamableHttpServerTransportTest {

  private JsonRpcCodec codec;

  private VertxMcpServer server;

  @BeforeEach
  void setUp(Vertx vertx) {
    this.codec = JsonRpcCodec.defaultCodec();
    this.server = VertxMcpServer.builder(vertx)
      .serverInfo("test-server", "9.9.9")
      .instructions("be brief")
      .tool(McpSchema.Tool.builder().name("echo").description("echoes").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder().build()))
      .build();
  }

  @Test
  void initializeReturnsSessionIdAndResult(Vertx vertx, VertxTestContext testContext) {
    StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, this.server);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(transport.router());

    HttpClient client = vertx.createHttpClient();
    vertx.createHttpServer()
      .requestHandler(router)
      .listen(0)
      .compose(httpServer -> post(client, httpServer.actualPort(), initializeBody()))
      .onComplete(testContext.succeeding(exchange -> testContext.verify(() -> {
        assertThat(exchange.status()).isEqualTo(200);
        assertThat(exchange.contentType()).startsWith("application/json");
        assertThat(exchange.sessionId()).isNotBlank();
        assertThat(transport.sessionCount()).isEqualTo(1);

        McpSchema.JSONRPCResponse response = (McpSchema.JSONRPCResponse) this.codec.decode(exchange.body());
        assertThat(response.jsonrpc()).isEqualTo(McpSchema.JSONRPC_VERSION);
        assertThat(response.id()).isEqualTo("init-1");
        assertThat(response.error()).isNull();

        McpSchema.InitializeResult result = this.codec.unmarshal(response.result(), McpSchema.InitializeResult.class);
        assertThat(result.protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_06_18);
        assertThat(result.serverInfo().name()).isEqualTo("test-server");
        assertThat(result.serverInfo().version()).isEqualTo("9.9.9");
        assertThat(result.instructions()).isEqualTo("be brief");
        assertThat(result.capabilities().tools()).isNotNull();

        testContext.completeNow();
      })));
  }

  @Test
  void postWithoutBothAcceptTypesIsRejected(Vertx vertx, VertxTestContext testContext) {
    StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, this.server);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(transport.router());

    HttpClient client = vertx.createHttpClient();
    vertx.createHttpServer()
      .requestHandler(router)
      .listen(0)
      .compose(httpServer -> client
        .request(new RequestOptions().setMethod(HttpMethod.POST)
          .setPort(httpServer.actualPort())
          .setHost("localhost")
          .setURI("/mcp")
          .putHeader("Accept", "application/json")
          .putHeader("Content-Type", "application/json"))
        .compose(request -> request.send(initializeBody()))
        .compose(response -> response.body().map(body -> response.statusCode())))
      .onComplete(testContext.succeeding(status -> testContext.verify(() -> {
        assertThat(status).isEqualTo(406);
        testContext.completeNow();
      })));
  }

  @Test
  void unknownSessionIsRejectedWithNotFound(Vertx vertx, VertxTestContext testContext) {
    StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, this.server);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(transport.router());

    Buffer body = this.codec
      .encodeToBuffer(new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION, "tools/list", "req-1", Map.of()));

    HttpClient client = vertx.createHttpClient();
    vertx.createHttpServer()
      .requestHandler(router)
      .listen(0)
      .compose(httpServer -> client
        .request(new RequestOptions().setMethod(HttpMethod.POST)
          .setPort(httpServer.actualPort())
          .setHost("localhost")
          .setURI("/mcp")
          .putHeader("Accept", "application/json, text/event-stream")
          .putHeader("Content-Type", "application/json")
          .putHeader(HttpHeaders.MCP_SESSION_ID, "nope"))
        .compose(request -> request.send(body))
        .compose(response -> response.body().map(buffer -> new Object[] { response.statusCode(), buffer })))
      .onComplete(testContext.succeeding(pair -> testContext.verify(() -> {
        assertThat(pair[0]).isEqualTo(404);
        McpSchema.JSONRPCResponse error = (McpSchema.JSONRPCResponse) this.codec.decode((Buffer) pair[1]);
        assertThat(error.error().code()).isEqualTo(HttpTransports.SESSION_NOT_FOUND);
        assertThat(error.error().message()).isEqualTo("Session not found");
        testContext.completeNow();
      })));
  }

  /**
   * The listening stream is the only way unsolicited server traffic reaches a client, so
   * it is worth proving end to end that a broadcast really lands as a well-formed SSE
   * frame -- and that DELETE then tears the session down.
   */
  @Test
  void listeningStreamReceivesBroadcastsAndDeleteTerminates(Vertx vertx, VertxTestContext testContext) {
    StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, this.server);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(transport.router());

    HttpClient client = vertx.createHttpClient();
    Checkpoint received = testContext.checkpoint();
    Checkpoint deleted = testContext.checkpoint();

    vertx.createHttpServer().requestHandler(router).listen(0).onComplete(testContext.succeeding(httpServer -> {
      int port = httpServer.actualPort();
      post(client, port, initializeBody()).onComplete(testContext.succeeding(init -> {
        String sessionId = init.sessionId();
        Buffer stream = Buffer.buffer();
        java.util.concurrent.atomic.AtomicBoolean seen = new java.util.concurrent.atomic.AtomicBoolean();
        client
          .request(new RequestOptions().setMethod(HttpMethod.GET)
            .setPort(port)
            .setHost("localhost")
            .setURI("/mcp")
            .putHeader("Accept", "text/event-stream")
            .putHeader(HttpHeaders.MCP_SESSION_ID, sessionId))
          .compose(request -> request.send())
          .onComplete(testContext.succeeding(response -> {
            testContext.verify(() -> {
              assertThat(response.statusCode()).isEqualTo(200);
              assertThat(response.getHeader("Content-Type")).isEqualTo("text/event-stream");
            });
            response.handler(chunk -> {
              stream.appendBuffer(chunk);
              String text = stream.toString();
              if (text.contains("notifications/tools/list_changed") && seen.compareAndSet(false, true)) {
                testContext.verify(() -> {
                  assertThat(text).contains("event: message\n");
                  assertThat(text).contains("data: {");
                  received.flag();
                });
                client
                  .request(new RequestOptions().setMethod(HttpMethod.DELETE)
                    .setPort(port)
                    .setHost("localhost")
                    .setURI("/mcp")
                    .putHeader(HttpHeaders.MCP_SESSION_ID, sessionId))
                  .compose(request -> request.send())
                  .onComplete(testContext.succeeding(deleteResponse -> testContext.verify(() -> {
                    assertThat(deleteResponse.statusCode()).isEqualTo(200);
                    assertThat(transport.sessionCount()).isZero();
                    deleted.flag();
                  })));
              }
            });
            // The listening stream is bound asynchronously on the session's context, so
            // give it a tick before broadcasting.
            vertx.setTimer(200, id -> this.server.notifyToolsListChanged());
          }));
      }));
    }));
  }

  private Buffer initializeBody() {
    McpSchema.InitializeRequest params = new McpSchema.InitializeRequest(ProtocolVersions.MCP_2025_06_18,
        new McpSchema.ClientCapabilities(null, null, null, null),
        new McpSchema.Implementation("test-client", "1.0.0"));
    return this.codec.encodeToBuffer(new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION,
        McpSchema.METHOD_INITIALIZE, "init-1", params));
  }

  private static Future<Exchange> post(HttpClient client, int port, Buffer body) {
    return client
      .request(new RequestOptions().setMethod(HttpMethod.POST)
        .setPort(port)
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader("Accept", "application/json, text/event-stream")
        .putHeader("Content-Type", "application/json"))
      .compose(request -> request.send(body))
      .compose(response -> response.body()
        .map(buffer -> new Exchange(response.statusCode(), response.getHeader("Content-Type"),
            response.getHeader(HttpHeaders.MCP_SESSION_ID), buffer)));
  }

  private record Exchange(int status, String contentType, String sessionId, Buffer body) {
  }
}
