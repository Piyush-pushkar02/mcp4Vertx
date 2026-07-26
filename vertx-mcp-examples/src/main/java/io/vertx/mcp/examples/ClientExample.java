package io.vertx.mcp.examples;

import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.client.VertxMcpClient;
import io.vertx.mcp.client.transport.StreamableHttpClientTransport;

/**
 * The client side of {@link WeatherServerVerticle}: connect, handshake, list what the
 * server offers, call one tool, print, hang up.
 *
 * <pre>{@code
 * mvn -pl vertx-mcp-examples exec:java \
 *     -Dexec.mainClass=io.vertx.mcp.examples.ClientExample \
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
