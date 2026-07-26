package io.vertx.mcp.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.client.VertxMcpClient;
import io.vertx.mcp.client.transport.StreamableHttpClientTransport;

/**
 * The property this class establishes: the conformance runs both ways. Here the
 * <em>official</em> SDK provides the server -- its servlet streamable-HTTP transport
 * hosted in Jetty -- and our {@link VertxMcpClient} is the one under test: it handshakes,
 * lists and calls tools, reads an exact and a templated resource and renders a prompt
 * against an implementation this repository did not write.
 *
 * <p>
 * The upstream server is blocking and servlet-based, so it is started and stopped on the
 * plain JUnit thread. Our client is future-based, so each test drives it from the JUnit
 * thread by awaiting its futures with an explicit timeout -- never by blocking a Vert.x
 * context.
 */
class ReverseConformanceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  /** See {@link OfficialSdkConformanceTest}: upstream's own validator cannot load here. */
  private static final JsonSchemaValidator PERMISSIVE_VALIDATOR = (schema,
      structuredContent) -> JsonSchemaValidator.ValidationResponse.asValid(null);

  private static Server jetty;

  private static McpSyncServer upstreamServer;

  private static Vertx vertx;

  private static int port;

  @BeforeAll
  static void bootUpstreamServer() throws Exception {
    HttpServletStreamableServerTransportProvider provider = HttpServletStreamableServerTransportProvider.builder()
      .mcpEndpoint("/mcp")
      .build();

    upstreamServer = McpServer.sync(provider)
      .serverInfo("upstream-server", "3.0.0")
      .instructions("served by the official sdk")
      .jsonSchemaValidator(PERMISSIVE_VALIDATOR)
      .tools(new McpServerFeatures.SyncToolSpecification(
          McpSchema.Tool.builder().name("echo").description("returns its text argument").build(),
          (exchange, request) -> McpSchema.CallToolResult.builder()
            .addTextContent(String.valueOf(request.arguments().get("text")))
            .build()))
      .resources(new McpServerFeatures.SyncResourceSpecification(
          McpSchema.Resource.builder()
            .uri("upstream://static")
            .name("static")
            .mimeType("text/plain")
            .build(),
          (exchange, request) -> new McpSchema.ReadResourceResult(
              List.of(new McpSchema.TextResourceContents("upstream://static", "text/plain", "upstream document")))))
      .resourceTemplates(new McpServerFeatures.SyncResourceTemplateSpecification(
          McpSchema.ResourceTemplate.builder().uriTemplate("upstream://item/{id}").name("item").build(),
          (exchange, request) -> new McpSchema.ReadResourceResult(
              List.of(new McpSchema.TextResourceContents(request.uri(), "text/plain",
                  "item " + request.uri().substring(request.uri().lastIndexOf('/') + 1))))))
      .prompts(new McpServerFeatures.SyncPromptSpecification(
          new McpSchema.Prompt("greet", "a greeting",
              List.of(new McpSchema.PromptArgument("name", "who to greet", true))),
          (exchange, request) -> new McpSchema.GetPromptResult("a greeting",
              List.of(new McpSchema.PromptMessage(McpSchema.Role.USER,
                  new McpSchema.TextContent("Hello, " + request.arguments().get("name")))))))
      .build();

    jetty = new Server();
    ServerConnector connector = new ServerConnector(jetty);
    connector.setPort(0);
    jetty.addConnector(connector);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    context.addServlet(new ServletHolder(provider), "/*");
    jetty.setHandler(context);
    jetty.start();
    port = connector.getLocalPort();

    vertx = Vertx.vertx();
  }

  @AfterAll
  static void stopUpstreamServer() throws Exception {
    if (vertx != null) {
      await(vertx.close());
    }
    if (upstreamServer != null) {
      upstreamServer.closeGracefully();
    }
    if (jetty != null) {
      jetty.stop();
    }
  }

  @Test
  void ourClientHandshakesWithTheOfficialServletServer() throws Exception {
    VertxMcpClient client = client();
    try {
      McpSchema.InitializeResult init = await(client.initialize());
      assertThat(init.serverInfo().name()).isEqualTo("upstream-server");
      assertThat(init.serverInfo().version()).isEqualTo("3.0.0");
      assertThat(init.instructions()).isEqualTo("served by the official sdk");
      assertThat(init.capabilities().tools()).isNotNull();
      assertThat(client.protocolVersion()).isNotBlank();
    }
    finally {
      await(client.close());
    }
  }

  @Test
  void ourClientListsAndCallsToolsOnTheOfficialServletServer() throws Exception {
    VertxMcpClient client = client();
    try {
      await(client.initialize());
      assertThat(await(client.listAllTools())).extracting(McpSchema.Tool::name).contains("echo");
      McpSchema.CallToolResult result = await(
          client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "from vertx"))));
      assertThat(((McpSchema.TextContent) result.content().get(0)).text()).isEqualTo("from vertx");
    }
    finally {
      await(client.close());
    }
  }

  @Test
  void ourClientReadsResourcesAndPromptsFromTheOfficialServletServer() throws Exception {
    VertxMcpClient client = client();
    try {
      await(client.initialize());

      assertThat(await(client.listAllResources())).extracting(McpSchema.Resource::uri)
        .contains("upstream://static");
      assertThat(text(await(client.readResource("upstream://static")))).isEqualTo("upstream document");
      assertThat(text(await(client.readResource("upstream://item/9")))).isEqualTo("item 9");

      McpSchema.GetPromptResult prompt = await(
          client.getPrompt(new McpSchema.GetPromptRequest("greet", Map.of("name", "Edsger"))));
      assertThat(((McpSchema.TextContent) prompt.messages().get(0).content()).text()).isEqualTo("Hello, Edsger");
    }
    finally {
      await(client.close());
    }
  }

  // --- fixtures ------------------------------------------------------------

  private static VertxMcpClient client() {
    return VertxMcpClient.builder(vertx)
      .transport(StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:" + port)
        .endpoint("/mcp")
        .requestTimeout(TIMEOUT)
        .build())
      .clientInfo("vertx-mcp-client", "1.0.0")
      .requestTimeout(TIMEOUT)
      .build();
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static String text(McpSchema.ReadResourceResult result) {
    return ((McpSchema.TextResourceContents) result.contents().get(0)).text();
  }
}
