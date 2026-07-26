package io.vertx.mcp.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientSseClientTransport;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.mcp.server.VertxMcpServer;
import io.vertx.mcp.server.transport.SseServerTransport;
import io.vertx.mcp.server.transport.StreamableHttpServerTransport;

/**
 * The property this class establishes: our server is conformant against the reference
 * implementation, not merely self-consistent. The <em>official</em>
 * {@code io.modelcontextprotocol.sdk} synchronous client -- a different codebase, a
 * different HTTP stack, a different JSON mapper -- drives a {@link VertxMcpServer} through
 * initialize, {@code tools/list}, {@code tools/call} (success, declared failure and
 * error), {@code resources/read} (exact and templated) and {@code prompts/get}, over both
 * the streamable HTTP transport and the legacy 2024-11-05 HTTP+SSE pair.
 *
 * <p>
 * The upstream client is Reactor-based and blocks on every call, so every test here runs
 * on the plain JUnit thread and never on a Vert.x context. The Vert.x server is booted
 * once for the class and awaited with an explicit timeout.
 *
 * <p>
 * One deliberate wrinkle: upstream's default {@code JsonSchemaValidator} needs
 * {@code com.networknt:json-schema-validator} 3.x, which shares its package with the 1.x
 * this repository's own validator compiles against. Only one can be on a classpath, so
 * the upstream client is handed a permissive validator here; what is under test is the
 * server's behaviour, and the server does its own output-schema validation.
 */
class OfficialSdkConformanceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  /** See the class javadoc: upstream's networknt-backed validator cannot load here. */
  private static final JsonSchemaValidator PERMISSIVE_VALIDATOR = (schema,
      structuredContent) -> JsonSchemaValidator.ValidationResponse.asValid(null);

  private static Vertx vertx;

  private static HttpServer streamableServer;

  private static HttpServer legacyServer;

  @BeforeAll
  static void bootVertxServer() throws Exception {
    vertx = Vertx.vertx();
    VertxMcpServer server = conformanceServer(vertx);

    Router streamableRouter = Router.router(vertx);
    streamableRouter.route("/mcp*")
      .subRouter(StreamableHttpServerTransport.create(vertx, server).router());
    streamableServer = await(vertx.createHttpServer().requestHandler(streamableRouter).listen(0));

    // The legacy transport owns two paths, so it gets its own port rather than a prefix.
    legacyServer = await(
        vertx.createHttpServer().requestHandler(SseServerTransport.create(vertx, server).router()).listen(0));
  }

  @AfterAll
  static void stopVertxServer() throws Exception {
    await(streamableServer.close());
    await(legacyServer.close());
    await(vertx.close());
  }

  // --- streamable HTTP -----------------------------------------------------

  @Test
  void theOfficialStreamableClientCompletesTheHandshake() {
    McpSyncClient client = streamableClient();
    try {
      McpSchema.InitializeResult init = client.initialize();
      assertThat(init.serverInfo().name()).isEqualTo("conformance-server");
      assertThat(init.serverInfo().version()).isEqualTo("2.0.0");
      assertThat(init.instructions()).isEqualTo("proving interoperability");
      assertThat(init.capabilities().tools()).isNotNull();
      assertThat(init.capabilities().resources()).isNotNull();
      assertThat(init.capabilities().prompts()).isNotNull();
      // Upstream offers 2025-11-25 as its newest; our server speaks it, so it must win.
      assertThat(init.protocolVersion()).isEqualTo("2025-11-25");
      assertThat(client.getServerInstructions()).isEqualTo("proving interoperability");
    }
    finally {
      client.closeGracefully();
    }
  }

  @Test
  void theOfficialStreamableClientListsAndCallsTools() {
    McpSyncClient client = streamableClient();
    try {
      client.initialize();

      McpSchema.ListToolsResult tools = client.listTools();
      assertThat(tools.tools()).extracting(McpSchema.Tool::name).contains("echo", "explode", "refuse");

      McpSchema.CallToolResult echoed = client
        .callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "from the official sdk")));
      assertThat(echoed.isError()).isNotEqualTo(Boolean.TRUE);
      assertThat(text(echoed)).isEqualTo("from the official sdk");

      // A tool that reports failure in-band: a normal result with isError set.
      McpSchema.CallToolResult refused = client.callTool(new McpSchema.CallToolRequest("refuse", Map.of()));
      assertThat(refused.isError()).isTrue();

      // A tool whose handler blows up: a JSON-RPC error, which upstream raises.
      assertThatThrownBy(() -> client.callTool(new McpSchema.CallToolRequest("explode", Map.of())))
        .isInstanceOf(McpError.class)
        .hasMessageContaining("detonated");
    }
    finally {
      client.closeGracefully();
    }
  }

  @Test
  void theOfficialStreamableClientReadsResourcesAndTemplates() {
    McpSyncClient client = streamableClient();
    try {
      client.initialize();

      assertThat(client.listResources().resources()).extracting(McpSchema.Resource::uri)
        .containsExactly("conformance://static");
      assertThat(client.listResourceTemplates().resourceTemplates())
        .extracting(McpSchema.ResourceTemplate::uriTemplate)
        .containsExactly("conformance://item/{id}");

      assertThat(resourceText(client.readResource(new McpSchema.ReadResourceRequest("conformance://static"))))
        .isEqualTo("a static document");
      assertThat(resourceText(client.readResource(new McpSchema.ReadResourceRequest("conformance://item/7"))))
        .isEqualTo("item 7");
    }
    finally {
      client.closeGracefully();
    }
  }

  @Test
  void theOfficialStreamableClientGetsPrompts() {
    McpSyncClient client = streamableClient();
    try {
      client.initialize();

      assertThat(client.listPrompts().prompts()).extracting(McpSchema.Prompt::name).containsExactly("greet");
      McpSchema.GetPromptResult prompt = client
        .getPrompt(new McpSchema.GetPromptRequest("greet", Map.of("name", "Grace")));
      assertThat(prompt.description()).isEqualTo("a greeting");
      assertThat(((McpSchema.TextContent) prompt.messages().get(0).content()).text()).isEqualTo("Hello, Grace");
    }
    finally {
      client.closeGracefully();
    }
  }

  // --- legacy 2024-11-05 HTTP+SSE ------------------------------------------

  @Test
  void theOfficialLegacySseClientInitializesAndCallsATool() {
    McpSyncClient client = McpClient
      .sync(HttpClientSseClientTransport.builder("http://localhost:" + legacyServer.actualPort())
        .sseEndpoint("/sse")
        .build())
      .clientInfo(new McpSchema.Implementation("official-sdk-sse-client", "1.0.0"))
      .requestTimeout(TIMEOUT)
      .initializationTimeout(TIMEOUT)
      .jsonSchemaValidator(PERMISSIVE_VALIDATOR)
      .build();
    try {
      McpSchema.InitializeResult init = client.initialize();
      assertThat(init.protocolVersion()).isEqualTo("2024-11-05");
      assertThat(init.serverInfo().name()).isEqualTo("conformance-server");

      assertThat(client.listTools().tools()).extracting(McpSchema.Tool::name).contains("echo");
      assertThat(text(client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "over legacy sse")))))
        .isEqualTo("over legacy sse");
    }
    finally {
      client.closeGracefully();
    }
  }

  // --- fixtures ------------------------------------------------------------

  private static McpSyncClient streamableClient() {
    return McpClient
      .sync(HttpClientStreamableHttpTransport.builder("http://localhost:" + streamableServer.actualPort())
        .endpoint("/mcp")
        .build())
      .clientInfo(new McpSchema.Implementation("official-sdk-client", "1.0.0"))
      .requestTimeout(TIMEOUT)
      .initializationTimeout(TIMEOUT)
      .jsonSchemaValidator(PERMISSIVE_VALIDATOR)
      .build();
  }

  private static VertxMcpServer conformanceServer(Vertx vertx) {
    return VertxMcpServer.builder(vertx)
      .serverInfo("conformance-server", "2.0.0")
      .instructions("proving interoperability")
      .tool(McpSchema.Tool.builder().name("echo").description("returns its text argument").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent(String.valueOf(request.arguments().get("text")))
            .build()))
      .tool(McpSchema.Tool.builder().name("explode").description("fails the JSON-RPC call").build(),
          (exchange, request) -> Future.failedFuture(new IllegalStateException("detonated")))
      .tool(McpSchema.Tool.builder().name("refuse").description("reports failure in-band").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent("not today")
            .isError(true)
            .build()))
      .resource(
          McpSchema.Resource.builder()
            .uri("conformance://static")
            .name("static")
            .mimeType("text/plain")
            .build(),
          (exchange, request) -> Future.succeededFuture(
              new McpSchema.ReadResourceResult(List.of(new McpSchema.TextResourceContents("conformance://static",
                  "text/plain", "a static document")))))
      .resourceTemplate(
          McpSchema.ResourceTemplate.builder().uriTemplate("conformance://item/{id}").name("item").build(),
          (exchange, request) -> Future.succeededFuture(new McpSchema.ReadResourceResult(
              List.of(new McpSchema.TextResourceContents(request.uri(), "text/plain",
                  "item " + request.uri().substring(request.uri().lastIndexOf('/') + 1))))))
      .prompt(
          new McpSchema.Prompt("greet", "a greeting",
              List.of(new McpSchema.PromptArgument("name", "who to greet", true))),
          (exchange, request) -> Future.succeededFuture(new McpSchema.GetPromptResult("a greeting",
              List.of(new McpSchema.PromptMessage(McpSchema.Role.USER,
                  new McpSchema.TextContent("Hello, " + request.arguments().get("name")))))))
      .build();
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static String text(McpSchema.CallToolResult result) {
    return ((McpSchema.TextContent) result.content().get(0)).text();
  }

  private static String resourceText(McpSchema.ReadResourceResult result) {
    return ((McpSchema.TextResourceContents) result.contents().get(0)).text();
  }
}
