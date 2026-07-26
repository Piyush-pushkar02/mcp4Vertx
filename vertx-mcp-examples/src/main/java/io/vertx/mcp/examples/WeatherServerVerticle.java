package io.vertx.mcp.examples;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.util.DefaultMcpUriTemplateManagerFactory;
import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.mcp.server.VertxMcpServer;
import io.vertx.mcp.server.transport.StreamableHttpServerTransport;

/**
 * A runnable MCP server over streamable HTTP, mounted as one sub-router inside an
 * ordinary vertx-web application that also serves {@code /health}. That composition is
 * the point: an MCP endpoint is just another route, not a server of its own.
 *
 * <pre>{@code
 * vertx.deployVerticle(new WeatherServerVerticle(8080));
 * # then, from a shell:
 * curl -s localhost:8080/health
 * curl -s localhost:8080/mcp -H 'Accept: application/json, text/event-stream' \
 *      -H 'Content-Type: application/json' \
 *      -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}'
 * }</pre>
 */
public class WeatherServerVerticle extends VerticleBase {

  /** Templated resource: one forecast document per city. */
  public static final String FORECAST_TEMPLATE = "weather://forecast/{city}";

  /** Static resource: the list of stations this server knows about. */
  public static final String STATIONS_URI = "weather://stations";

  private final int port;

  private VertxMcpServer mcpServer;

  private HttpServer httpServer;

  public WeatherServerVerticle() {
    this(8080);
  }

  /** Pass {@code 0} to let the OS pick a port, then read it back from {@link #port()}. */
  public WeatherServerVerticle(int port) {
    this.port = port;
  }

  /** The port the HTTP server actually bound to. Only meaningful after deployment. */
  public int port() {
    return this.httpServer.actualPort();
  }

  /** The MCP server behind the endpoint, so callers can add tools at runtime. */
  public VertxMcpServer mcpServer() {
    return this.mcpServer;
  }

  @Override
  public Future<?> start() {
    this.mcpServer = buildMcpServer(this.vertx);

    Router app = Router.router(this.vertx);
    // A plain, non-MCP route. It shares the port and the router with the MCP endpoint.
    app.get("/health").handler(rc -> rc.json(new JsonObject().put("status", "UP")));
    app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(this.vertx, this.mcpServer).router());

    return this.vertx.createHttpServer().requestHandler(app).listen(this.port).onSuccess(server -> {
      this.httpServer = server;
    });
  }

  @Override
  public Future<?> stop() {
    return this.httpServer == null ? Future.succeededFuture() : this.httpServer.close();
  }

  /** Builds the server's feature set. Public so tests and other examples can reuse it. */
  public static VertxMcpServer buildMcpServer(Vertx vertx) {
    return VertxMcpServer.builder(vertx)
      .serverInfo("weather", "1.0.0")
      .instructions("Ask for a forecast by city, or read weather://forecast/<city> directly.")
      .tool(forecastTool(), WeatherServerVerticle::forecast)
      .toolBlocking(windChillTool(), WeatherServerVerticle::windChill)
      .resource(stationsResource(), (exchange, request) -> Future.succeededFuture(new McpSchema.ReadResourceResult(
          List.of(new McpSchema.TextResourceContents(STATIONS_URI, "text/plain", "LFPG\nEGLL\nKJFK")))))
      .resourceTemplate(forecastTemplate(), WeatherServerVerticle::readForecast)
      .prompt(travelPrompt(), WeatherServerVerticle::travelAdvice)
      .build();
  }

  // --- tools ---------------------------------------------------------------

  private static McpSchema.Tool forecastTool() {
    return McpSchema.Tool.builder()
      .name("forecast")
      .description("Multi-day forecast for a city. Reports progress while it works.")
      .inputSchema(new McpSchema.JsonSchema("object", Map.of("city", Map.of("type", "string")), List.of("city"),
          false, null, null))
      .build();
  }

  /**
   * An asynchronous tool. Every {@code progress} call is written onto the very SSE
   * response this call arrived on, so the client sees them before the result -- and the
   * event loop is never blocked between them.
   */
  private static Future<McpSchema.CallToolResult> forecast(io.vertx.mcp.server.McpServerExchange exchange,
      McpSchema.CallToolRequest request) {
    String city = argument(request, "city", "nowhere");
    return exchange.progress(0.0, 3.0, "resolving " + city)
      .compose(v -> exchange.progress(1.0, 3.0, "querying stations"))
      .compose(v -> exchange.progress(2.0, 3.0, "rendering"))
      .compose(v -> exchange.log(McpSchema.LoggingLevel.INFO, "forecast", "forecast for " + city + " ready"))
      .map(v -> McpSchema.CallToolResult.builder().addTextContent(forecastFor(city)).build());
  }

  private static McpSchema.Tool windChillTool() {
    return McpSchema.Tool.builder()
      .name("wind-chill")
      .description("Wind chill in degrees Celsius. Deliberately written in blocking style.")
      .inputSchema(new McpSchema.JsonSchema("object",
          Map.of("celsius", Map.of("type", "number"), "windKph", Map.of("type", "number")),
          List.of("celsius", "windKph"), false, null, null))
      .build();
  }

  /**
   * A blocking tool. Registered with {@code toolBlocking}, so this method runs on a
   * worker thread and may call whatever synchronous library it likes.
   */
  private static McpSchema.CallToolResult windChill(io.vertx.mcp.server.McpServerExchange exchange,
      McpSchema.CallToolRequest request) {
    double celsius = number(request, "celsius", 0);
    double windKph = number(request, "windKph", 0);
    double v = Math.pow(windKph, 0.16);
    double chill = 13.12 + 0.6215 * celsius - 11.37 * v + 0.3965 * celsius * v;
    return McpSchema.CallToolResult.builder()
      .addTextContent(String.format("%.1f", chill))
      .build();
  }

  // --- resources -----------------------------------------------------------

  private static McpSchema.Resource stationsResource() {
    return McpSchema.Resource.builder()
      .uri(STATIONS_URI)
      .name("stations")
      .description("Every station this server can report on")
      .mimeType("text/plain")
      .build();
  }

  private static McpSchema.ResourceTemplate forecastTemplate() {
    return McpSchema.ResourceTemplate.builder()
      .uriTemplate(FORECAST_TEMPLATE)
      .name("forecast")
      .description("The forecast document for one city")
      .mimeType("text/plain")
      .build();
  }

  private static Future<McpSchema.ReadResourceResult> readForecast(io.vertx.mcp.server.McpServerExchange exchange,
      McpSchema.ReadResourceRequest request) {
    Map<String, String> variables = new DefaultMcpUriTemplateManagerFactory().create(FORECAST_TEMPLATE)
      .extractVariableValues(request.uri());
    String city = variables.getOrDefault("city", "nowhere");
    return Future.succeededFuture(new McpSchema.ReadResourceResult(
        List.of(new McpSchema.TextResourceContents(request.uri(), "text/plain", forecastFor(city)))));
  }

  // --- prompts -------------------------------------------------------------

  private static McpSchema.Prompt travelPrompt() {
    return new McpSchema.Prompt("travel-advice", "Should I pack a coat?",
        List.of(new McpSchema.PromptArgument("city", "Where you are going", true)));
  }

  private static Future<McpSchema.GetPromptResult> travelAdvice(io.vertx.mcp.server.McpServerExchange exchange,
      McpSchema.GetPromptRequest request) {
    Object city = request.arguments() == null ? null : request.arguments().get("city");
    return Future.succeededFuture(new McpSchema.GetPromptResult("Packing advice",
        List.of(new McpSchema.PromptMessage(McpSchema.Role.USER,
            new McpSchema.TextContent("I am travelling to " + city + ". What should I pack?")))));
  }

  // --- helpers -------------------------------------------------------------

  private static String forecastFor(String city) {
    return city + ": 18C, light rain, wind 12 kph";
  }

  private static String argument(McpSchema.CallToolRequest request, String name, String fallback) {
    Object value = request.arguments() == null ? null : request.arguments().get(name);
    return value == null ? fallback : value.toString();
  }

  private static double number(McpSchema.CallToolRequest request, String name, double fallback) {
    Object value = request.arguments() == null ? null : request.arguments().get(name);
    return value instanceof Number n ? n.doubleValue() : fallback;
  }
}
