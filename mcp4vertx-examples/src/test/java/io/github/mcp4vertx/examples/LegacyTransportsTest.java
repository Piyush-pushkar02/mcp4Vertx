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
package io.github.mcp4vertx.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.ext.web.Router;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.github.mcp4vertx.client.VertxMcpClient;
import io.github.mcp4vertx.client.transport.SseClientTransport;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.server.transport.SseServerTransport;
import io.github.mcp4vertx.server.transport.StatelessHttpServerTransport;
import io.github.mcp4vertx.spec.JsonRpcCodec;

/**
 * The property this class establishes: the two non-streamable transports still work end
 * to end. The 2024-11-05 HTTP+SSE pair completes a handshake over its split
 * {@code GET /sse} + {@code POST /message} channels and can call a tool; the stateless
 * transport answers a bare {@code POST} with a plain JSON-RPC result and refuses anything
 * that would require it to remember the caller.
 */
@ExtendWith(VertxExtension.class)
class LegacyTransportsTest {

  private final JsonRpcCodec codec = JsonRpcCodec.defaultCodec();

  private VertxMcpServer server(Vertx vertx) {
    return VertxMcpServer.builder(vertx)
      .serverInfo("legacy-server", "0.9.0")
      .instructions("older protocols welcome")
      .tool(McpSchema.Tool.builder().name("echo").description("returns its text argument").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent(String.valueOf(request.arguments().get("text")))
            .build()))
      .build();
  }

  // --- 2024-11-05 HTTP+SSE -------------------------------------------------

  @Test
  void theLegacySseTransportCompletesAHandshakeAndCallsATool(Vertx vertx, VertxTestContext testContext) {
    VertxMcpServer mcpServer = server(vertx);
    SseServerTransport transport = SseServerTransport.create(vertx, mcpServer);

    vertx.createHttpServer().requestHandler(transport.router()).listen(0).compose(http -> {
      VertxMcpClient client = VertxMcpClient.builder(vertx)
        .transport(SseClientTransport.builder(vertx)
          .baseUrl("http://localhost:" + http.actualPort())
          .sseEndpoint("/sse")
          .build())
        .clientInfo("legacy-client", "1.0.0")
        .build();
      return client.initialize().compose(init -> {
        testContext.verify(() -> {
          assertThat(init.protocolVersion()).isEqualTo(ProtocolVersions.MCP_2024_11_05);
          assertThat(init.serverInfo().name()).isEqualTo("legacy-server");
          assertThat(init.instructions()).isEqualTo("older protocols welcome");
          assertThat(transport.sessionCount()).isEqualTo(1);
        });
        return client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "over sse")));
      }).eventually(client::close).eventually(http::close);
    }).onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(((McpSchema.TextContent) result.content().get(0)).text()).isEqualTo("over sse");
      testContext.completeNow();
    })));
  }

  // --- stateless -----------------------------------------------------------

  @Test
  void theStatelessTransportAnswersAPostInlineAndRejectsGet(Vertx vertx, VertxTestContext testContext) {
    VertxMcpServer mcpServer = server(vertx);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(StatelessHttpServerTransport.create(vertx, mcpServer).router());
    HttpClient http = vertx.createHttpClient();

    vertx.createHttpServer().requestHandler(router).listen(0).compose(server -> {
      int port = server.actualPort();
      return post(http, port, new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION,
          McpSchema.METHOD_INITIALIZE, "init-1",
          new McpSchema.InitializeRequest(ProtocolVersions.MCP_2025_06_18,
              new McpSchema.ClientCapabilities(null, null, null, null),
              new McpSchema.Implementation("raw", "1.0.0"))))
        .compose(initialize -> {
          testContext.verify(() -> {
            assertThat(initialize.status()).isEqualTo(200);
            assertThat(initialize.contentType()).startsWith("application/json");
            McpSchema.JSONRPCResponse response = decode(initialize.body());
            assertThat(response.error()).isNull();
            assertThat(response.id()).isEqualTo("init-1");
            McpSchema.InitializeResult result = this.codec.unmarshal(response.result(),
                McpSchema.InitializeResult.class);
            assertThat(result.serverInfo().name()).isEqualTo("legacy-server");
          });
          return post(http, port, new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION,
              McpSchema.METHOD_TOOLS_CALL, "call-1",
              new McpSchema.CallToolRequest("echo", Map.of("text", "stateless"))));
        })
        .compose(call -> {
          testContext.verify(() -> {
            assertThat(call.status()).isEqualTo(200);
            McpSchema.JSONRPCResponse response = decode(call.body());
            assertThat(response.error()).isNull();
            McpSchema.CallToolResult result = this.codec.unmarshal(response.result(),
                McpSchema.CallToolResult.class);
            assertThat(((McpSchema.TextContent) result.content().get(0)).text()).isEqualTo("stateless");
          });
          return get(http, port);
        })
        .compose(rejected -> {
          testContext.verify(() -> {
            assertThat(rejected.status()).isEqualTo(405);
            McpSchema.JSONRPCResponse response = decode(rejected.body());
            assertThat(response.error().message()).contains("only POST is supported");
          });
          return server.close();
        });
    }).onComplete(testContext.succeedingThenComplete());
  }

  // --- plumbing ------------------------------------------------------------

  private record Exchange(int status, String contentType, Buffer body) {
  }

  private Future<Exchange> post(HttpClient http, int port, Object message) {
    return http
      .request(new RequestOptions().setMethod(HttpMethod.POST)
        .setPort(port)
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader("Accept", "application/json, text/event-stream")
        .putHeader("Content-Type", "application/json"))
      .compose(request -> request.send(this.codec.encodeToBuffer(message)))
      .compose(response -> response.body()
        .map(body -> new Exchange(response.statusCode(), String.valueOf(response.getHeader("Content-Type")),
            body)));
  }

  private Future<Exchange> get(HttpClient http, int port) {
    return http
      .request(new RequestOptions().setMethod(HttpMethod.GET)
        .setPort(port)
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader("Accept", "text/event-stream"))
      .compose(request -> request.send())
      .compose(response -> response.body()
        .map(body -> new Exchange(response.statusCode(), String.valueOf(response.getHeader("Content-Type")),
            body)));
  }

  private McpSchema.JSONRPCResponse decode(Buffer body) {
    try {
      return (McpSchema.JSONRPCResponse) this.codec.decode(body);
    }
    catch (Exception e) {
      throw new AssertionError("Not a JSON-RPC response: " + body, e);
    }
  }
}
