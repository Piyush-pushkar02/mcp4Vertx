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

import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.github.mcp4vertx.client.VertxMcpClient;
import io.github.mcp4vertx.client.transport.StreamableHttpClientTransport;

/**
 * The client side of {@link WeatherServerVerticle}: connect, handshake, list what the
 * server offers, call one tool, print, hang up.
 *
 * <pre>{@code
 * mvn -pl mcp4vertx-examples exec:java \
 *     -Dexec.mainClass=io.github.mcp4vertx.examples.ClientExample \
 *     -Dexec.args=http://localhost:8080
 * }</pre>
 */
public final class ClientExample {

  private ClientExample() {
  }

  public static void main(String[] args) {
    String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
    Vertx vertx = Vertx.vertx();
    run(vertx, baseUrl, System.out::println).onComplete(result -> {
      if (result.failed()) {
        result.cause().printStackTrace();
      }
      vertx.close();
    });
  }

  /**
   * The whole conversation as one future chain. Extracted from {@code main} so tests can
   * drive it against a server on an ephemeral port and capture what it printed.
   */
  public static Future<Void> run(Vertx vertx, String baseUrl, java.util.function.Consumer<String> out) {
    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(StreamableHttpClientTransport.builder(vertx).baseUrl(baseUrl).endpoint("/mcp").build())
      .clientInfo("weather-cli", "1.0.0")
      .progressHandler(progress -> out.accept("  .. " + progress.progress() + "/" + progress.total() + " "
          + progress.message()))
      .build();

    return client.initialize().compose(init -> {
      out.accept("connected to " + init.serverInfo().name() + " " + init.serverInfo().version()
          + " speaking " + init.protocolVersion());
      out.accept("instructions: " + init.instructions());
      return client.listAllTools();
    }).compose(tools -> {
      tools.forEach(tool -> out.accept("tool: " + tool.name() + " - " + tool.description()));
      return client.callTool(McpSchema.CallToolRequest.builder()
        .name("forecast")
        .arguments(Map.of("city", "Paris"))
        // Asking for progress is opt-in: without a token the server stays quiet.
        .meta(Map.of("progressToken", "forecast-1"))
        .build());
    }).compose(result -> {
      result.content().forEach(content -> {
        if (content instanceof McpSchema.TextContent text) {
          out.accept("forecast: " + text.text());
        }
      });
      return Future.<Void>succeededFuture();
    }).eventually(client::close);
  }
}
