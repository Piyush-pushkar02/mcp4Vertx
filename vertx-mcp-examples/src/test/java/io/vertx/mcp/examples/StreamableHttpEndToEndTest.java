package io.vertx.mcp.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.RequestOptions;
import io.vertx.ext.web.Router;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mcp.client.VertxMcpClient;
import io.vertx.mcp.client.transport.McpSessionNotFoundException;
import io.vertx.mcp.client.transport.StreamableHttpClientTransport;
import io.vertx.mcp.server.McpServerExchange;
import io.vertx.mcp.server.VertxMcpServer;
import io.vertx.mcp.server.transport.StreamableHttpServerTransport;

/**
 * The property this class establishes: a {@link VertxMcpClient} talking to a
 * {@link VertxMcpServer} over streamable HTTP and a real socket implements the whole of
 * the MCP 2025-06-18 request surface correctly -- handshake, tools (including failures
 * and structured output), resources, prompts, logging levels, list-changed broadcasts and
 * session termination -- and gets the two hard streaming properties right: notifications
 * a handler emits mid-call arrive on the same POST response ahead of the result, and a
 * server-to-client {@code sampling/createMessage} issued from inside a tool handler is
 * answered on the per-request stream and correlated back to that handler.
 *
 * <p>
 * Nothing here sleeps. Ordering assertions rely on the fact that a single SSE response is
 * delivered in order on one Vert.x context, so by the time a call's future completes
 * everything the handler wrote before the result has already reached the client's
 * handlers.
 */
@ExtendWith(VertxExtension.class)
class StreamableHttpEndToEndTest {

  private static final Map<String, Object> TOTAL_SCHEMA = Map.of("type", "object", "properties",
      Map.of("total", Map.of("type", "number")), "required", List.of("total"));

  private VertxMcpServer server;

  private HttpServer httpServer;

  private String baseUrl;

  private final List<VertxMcpClient> clients = new ArrayList<>();

  @BeforeEach
  void startServer(Vertx vertx, VertxTestContext testContext) {
    this.server = fixtureServer(vertx);
    Router router = Router.router(vertx);
    router.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, this.server).router());
    vertx.createHttpServer().requestHandler(router).listen(0).onComplete(testContext.succeeding(http -> {
      this.httpServer = http;
      this.baseUrl = "http://localhost:" + http.actualPort();
      testContext.completeNow();
    }));
  }

  @AfterEach
  void stopServer(VertxTestContext testContext) {
    List<Future<?>> closes = new ArrayList<>();
    this.clients.forEach(client -> closes.add(client.close().recover(err -> Future.succeededFuture())));
    Future.all(closes).compose(v -> this.httpServer.close()).onComplete(testContext.succeedingThenComplete());
  }

  // --- a. handshake --------------------------------------------------------

  @Test
  void initializeNegotiatesAVersionAndReportsCapabilitiesAndInstructions(Vertx vertx,
      VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize().onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(result.protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_06_18);
      assertThat(result.serverInfo().name()).isEqualTo("e2e-server");
      assertThat(result.serverInfo().version()).isEqualTo("1.2.3");
      assertThat(result.instructions()).isEqualTo("this server exists to be tested");
      assertThat(result.capabilities().tools()).isNotNull();
      assertThat(result.capabilities().resources()).isNotNull();
      assertThat(result.capabilities().prompts()).isNotNull();
      assertThat(result.capabilities().logging()).isNotNull();
      assertThat(client.protocolVersion()).isEqualTo(result.protocolVersion());
      testContext.completeNow();
    })));
  }

  // --- b. tools ------------------------------------------------------------

  @Test
  void toolsListReturnsEveryRegisteredToolAndCallEchoesItsArgument(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.listAllTools())
      .compose(tools -> {
        testContext.verify(() -> assertThat(tools).extracting(McpSchema.Tool::name)
          .contains("echo", "boom", "sum", "bad-sum", "slow", "ask", "noisy"));
        return client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "hello")));
      })
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
        assertThat(textOf(result)).isEqualTo("hello");
        testContext.completeNow();
      })));
  }

  @Test
  void aToolThatFailsSurfacesAsAJsonRpcError(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("boom", Map.of())))
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpError.class);
        McpError error = (McpError) cause;
        assertThat(error.getJsonRpcError().code()).isEqualTo(McpSchema.ErrorCodes.INTERNAL_ERROR);
        assertThat(error.getJsonRpcError().message()).contains("kaboom");
        testContext.completeNow();
      })));
  }

  // --- c. structured output ------------------------------------------------

  @Test
  void aToolWithAnOutputSchemaReturnsStructuredContent(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("sum", Map.of("a", 2, "b", 3))))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result.structuredContent()).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) result.structuredContent()).get("total")).isEqualTo(5);
        testContext.completeNow();
      })));
  }

  @Test
  void structuredContentViolatingTheOutputSchemaFailsWithAnInternalError(Vertx vertx,
      VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("bad-sum", Map.of())))
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpError.class);
        McpError error = (McpError) cause;
        assertThat(error.getJsonRpcError().code()).isEqualTo(McpSchema.ErrorCodes.INTERNAL_ERROR);
        assertThat(error.getJsonRpcError().message()).contains("violates its outputSchema");
        testContext.completeNow();
      })));
  }

  // --- d. progress on the POST response ------------------------------------

  @Test
  void progressNotificationsArriveOnThePostStreamBeforeTheResult(Vertx vertx, VertxTestContext testContext) {
    List<McpSchema.ProgressNotification> progress = new CopyOnWriteArrayList<>();
    VertxMcpClient client = client(vertx, builder -> builder.progressHandler(progress::add));

    client.initialize()
      .compose(init -> client.callTool(McpSchema.CallToolRequest.builder()
        .name("slow")
        .arguments(Map.of())
        .meta(Map.of("progressToken", "tok-1"))
        .build()))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        // The result future completing is the synchronisation point: everything the
        // handler wrote onto this response earlier is already here.
        assertThat(progress).hasSize(3);
        assertThat(progress).extracting(McpSchema.ProgressNotification::message)
          .containsExactly("step 1", "step 2", "step 3");
        assertThat(progress).extracting(McpSchema.ProgressNotification::progressToken)
          .containsOnly("tok-1");
        assertThat(textOf(result)).isEqualTo("done");
        testContext.completeNow();
      })));
  }

  @Test
  void noProgressIsSentWhenTheClientSuppliesNoToken(Vertx vertx, VertxTestContext testContext) {
    List<McpSchema.ProgressNotification> progress = new CopyOnWriteArrayList<>();
    VertxMcpClient client = client(vertx, builder -> builder.progressHandler(progress::add));

    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("slow", Map.of())))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(progress).isEmpty();
        assertThat(textOf(result)).isEqualTo("done");
        testContext.completeNow();
      })));
  }

  // --- e. server-initiated sampling from inside a tool ---------------------

  @Test
  void aToolCanAskTheClientToSampleAndUseTheAnswer(Vertx vertx, VertxTestContext testContext) {
    List<McpSchema.CreateMessageRequest> seen = new CopyOnWriteArrayList<>();
    VertxMcpClient client = client(vertx, builder -> builder.sampling(request -> {
      seen.add(request);
      String question = ((McpSchema.TextContent) request.messages().get(0).content()).text();
      return Future.succeededFuture(new McpSchema.CreateMessageResult(McpSchema.Role.ASSISTANT,
          new McpSchema.TextContent("the answer to '" + question + "' is 4"), "stub-model",
          McpSchema.CreateMessageResult.StopReason.END_TURN));
    }));

    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("ask", Map.of("question", "2+2?"))))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(seen).hasSize(1);
        assertThat(seen.get(0).systemPrompt()).isEqualTo("be terse");
        assertThat(textOf(result)).isEqualTo("model said: the answer to '2+2?' is 4");
        testContext.completeNow();
      })));
  }

  @Test
  void samplingFailsCleanlyWhenTheClientDeclaresNoSamplingCapability(Vertx vertx,
      VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("ask", Map.of("question", "2+2?"))))
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpError.class);
        assertThat(((McpError) cause).getJsonRpcError().message()).contains("does not support sampling");
        testContext.completeNow();
      })));
  }

  // --- f. resources --------------------------------------------------------

  @Test
  void resourcesCanBeListedReadExactlyAndReadThroughATemplate(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.listAllResources())
      .compose(resources -> {
        testContext.verify(() -> assertThat(resources).extracting(McpSchema.Resource::uri)
          .containsExactly("test://static"));
        return client.listAllResourceTemplates();
      })
      .compose(templates -> {
        testContext.verify(() -> assertThat(templates).extracting(McpSchema.ResourceTemplate::uriTemplate)
          .containsExactly("test://item/{id}"));
        return client.readResource("test://static");
      })
      .compose(exact -> {
        testContext.verify(() -> assertThat(textOf(exact)).isEqualTo("a static document"));
        return client.readResource("test://item/42");
      })
      .onComplete(testContext.succeeding(templated -> testContext.verify(() -> {
        assertThat(textOf(templated)).isEqualTo("item 42");
        testContext.completeNow();
      })));
  }

  @Test
  void readingAnUnknownUriFailsWithResourceNotFound(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize()
      .compose(init -> client.readResource("test://nothing-here"))
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpError.class);
        assertThat(((McpError) cause).getJsonRpcError().code())
          .isEqualTo(McpSchema.ErrorCodes.RESOURCE_NOT_FOUND);
        testContext.completeNow();
      })));
  }

  // --- g. prompts ----------------------------------------------------------

  @Test
  void promptsCanBeListedAndRendered(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    client.initialize().compose(init -> client.listAllPrompts()).compose(prompts -> {
      testContext.verify(() -> {
        assertThat(prompts).extracting(McpSchema.Prompt::name).containsExactly("greet");
        assertThat(prompts.get(0).arguments()).extracting(McpSchema.PromptArgument::name).containsExactly("name");
      });
      return client.getPrompt(new McpSchema.GetPromptRequest("greet", Map.of("name", "Ada")));
    }).onComplete(testContext.succeeding(prompt -> testContext.verify(() -> {
      assertThat(prompt.description()).isEqualTo("a greeting");
      assertThat(prompt.messages()).hasSize(1);
      assertThat(((McpSchema.TextContent) prompt.messages().get(0).content()).text()).isEqualTo("Hello, Ada");
      testContext.completeNow();
    })));
  }

  // --- h. logging levels ---------------------------------------------------

  @Test
  void settingTheLogLevelToErrorFiltersOutInfoMessages(Vertx vertx, VertxTestContext testContext) {
    List<McpSchema.LoggingMessageNotification> logs = new CopyOnWriteArrayList<>();
    VertxMcpClient client = client(vertx, builder -> builder.loggingHandler(logs::add));

    client.initialize()
      // Default minimum is INFO, so both lines the tool emits get through.
      .compose(init -> client.callTool(new McpSchema.CallToolRequest("noisy", Map.of())))
      .compose(first -> {
        testContext.verify(() -> assertThat(logs).extracting(McpSchema.LoggingMessageNotification::level)
          .containsExactly(McpSchema.LoggingLevel.INFO, McpSchema.LoggingLevel.ERROR));
        logs.clear();
        return client.setLoggingLevel(McpSchema.LoggingLevel.ERROR);
      })
      .compose(v -> client.callTool(new McpSchema.CallToolRequest("noisy", Map.of())))
      .onComplete(testContext.succeeding(second -> testContext.verify(() -> {
        // The ERROR line rides the same ordered stream as the result, so seeing the
        // result proves the INFO line was dropped rather than merely delayed.
        assertThat(logs).hasSize(1);
        assertThat(logs.get(0).level()).isEqualTo(McpSchema.LoggingLevel.ERROR);
        assertThat(logs.get(0).data()).isEqualTo("an error line");
        testContext.completeNow();
      })));
  }

  // --- i. list-changed broadcast -------------------------------------------

  @Test
  void addingAToolBroadcastsListChangedOnTheListeningStream(Vertx vertx, VertxTestContext testContext) {
    AtomicBoolean fired = new AtomicBoolean();
    AtomicLong announcer = new AtomicLong(-1);
    VertxMcpClient client = client(vertx, builder -> builder.toolsChangeHandler(() -> {
      if (fired.compareAndSet(false, true)) {
        long timer = announcer.getAndSet(-1);
        if (timer >= 0) {
          vertx.cancelTimer(timer);
        }
        testContext.completeNow();
      }
    }));

    client.initialize()
      .compose(init -> this.server.addTool(
          McpSchema.Tool.builder().name("late-arrival").description("added at runtime").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder().build())))
      .onComplete(testContext.succeeding(v -> {
        // The standalone GET stream is opened lazily, once the session id exists, and
        // nothing on the wire tells the server when it is up. Rather than sleep, keep
        // re-announcing until the client acknowledges by firing its handler -- the
        // notification carries no payload, so a repeat costs nothing.
        if (!fired.get()) {
          announcer.set(vertx.setPeriodic(25, id -> this.server.notifyToolsListChanged()));
        }
      }));
  }

  // --- j. session termination ----------------------------------------------

  @Test
  void deleteTerminatesTheSessionAndLaterRequestsAreRejected(Vertx vertx, VertxTestContext testContext) {
    StreamableHttpClientTransport transport = StreamableHttpClientTransport.builder(vertx)
      .baseUrl(this.baseUrl)
      .endpoint("/mcp")
      .openListeningStream(false)
      .build();
    VertxMcpClient client = VertxMcpClient.builder(vertx).transport(transport).clientInfo("e2e", "1.0.0").build();
    this.clients.add(client);
    HttpClient rawClient = vertx.createHttpClient();

    client.initialize()
      .compose(init -> client.listTools())
      .compose(tools -> delete(rawClient, transport.sessionId()))
      .compose(status -> {
        testContext.verify(() -> assertThat(status).isEqualTo(200));
        return client.listTools();
      })
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpSessionNotFoundException.class);
        testContext.completeNow();
      })));
  }

  // --- k. concurrency ------------------------------------------------------

  @Test
  void twentyConcurrentToolCallsOnOneSessionEachGetTheirOwnResult(Vertx vertx, VertxTestContext testContext) {
    VertxMcpClient client = client(vertx, builder -> {
    });
    int calls = 20;

    client.initialize().compose(init -> {
      List<Future<?>> pending = new ArrayList<>();
      for (int i = 0; i < calls; i++) {
        String expected = "call-" + i;
        pending.add(client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", expected)))
          .map(result -> {
            testContext.verify(() -> assertThat(textOf(result)).isEqualTo(expected));
            return expected;
          }));
      }
      return Future.all(pending);
    }).onComplete(testContext.succeeding(composite -> testContext.verify(() -> {
      assertThat(composite.size()).isEqualTo(calls);
      for (int i = 0; i < calls; i++) {
        assertThat(composite.<String>resultAt(i)).isEqualTo("call-" + i);
      }
      testContext.completeNow();
    })));
  }

  // --- fixtures ------------------------------------------------------------

  private VertxMcpClient client(Vertx vertx, Consumer<VertxMcpClient.Builder> customizer) {
    VertxMcpClient.Builder builder = VertxMcpClient.builder(vertx)
      .transport(
          StreamableHttpClientTransport.builder(vertx).baseUrl(this.baseUrl).endpoint("/mcp").build())
      .clientInfo("e2e-client", "1.0.0");
    customizer.accept(builder);
    VertxMcpClient client = builder.build();
    this.clients.add(client);
    return client;
  }

  private Future<Integer> delete(HttpClient rawClient, String sessionId) {
    return rawClient
      .request(new RequestOptions().setMethod(HttpMethod.DELETE)
        .setPort(this.httpServer.actualPort())
        .setHost("localhost")
        .setURI("/mcp")
        .putHeader(HttpHeaders.MCP_SESSION_ID, sessionId))
      .compose(request -> request.send())
      .compose(response -> response.body().map(body -> response.statusCode()));
  }

  private static VertxMcpServer fixtureServer(Vertx vertx) {
    return VertxMcpServer.builder(vertx)
      .serverInfo("e2e-server", "1.2.3")
      .instructions("this server exists to be tested")
      .tool(McpSchema.Tool.builder().name("echo").description("returns its text argument").build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent(String.valueOf(request.arguments().get("text")))
            .build()))
      .tool(McpSchema.Tool.builder().name("boom").description("always fails").build(),
          (exchange, request) -> Future.failedFuture(new IllegalStateException("kaboom")))
      .tool(
          McpSchema.Tool.builder()
            .name("sum")
            .description("adds two numbers")
            .outputSchema(TOTAL_SCHEMA)
            .build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent("sum computed")
            .structuredContent(Map.of("total",
                ((Number) request.arguments().get("a")).intValue()
                    + ((Number) request.arguments().get("b")).intValue()))
            .build()))
      .tool(
          McpSchema.Tool.builder()
            .name("bad-sum")
            .description("returns output its own schema forbids")
            .outputSchema(TOTAL_SCHEMA)
            .build(),
          (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
            .addTextContent("sum computed")
            .structuredContent(Map.of("total", "twelve"))
            .build()))
      .tool(McpSchema.Tool.builder().name("slow").description("reports progress as it goes").build(),
          StreamableHttpEndToEndTest::slow)
      .tool(McpSchema.Tool.builder().name("ask").description("asks the client's model").build(),
          StreamableHttpEndToEndTest::ask)
      .tool(McpSchema.Tool.builder().name("noisy").description("emits one INFO and one ERROR log").build(),
          StreamableHttpEndToEndTest::noisy)
      .resource(
          McpSchema.Resource.builder().uri("test://static").name("static").mimeType("text/plain").build(),
          (exchange, request) -> Future.succeededFuture(new McpSchema.ReadResourceResult(
              List.of(new McpSchema.TextResourceContents("test://static", "text/plain", "a static document")))))
      .resourceTemplate(
          McpSchema.ResourceTemplate.builder().uriTemplate("test://item/{id}").name("item").build(),
          (exchange, request) -> Future.succeededFuture(new McpSchema.ReadResourceResult(
              List.of(new McpSchema.TextResourceContents(request.uri(), "text/plain",
                  "item " + request.uri().substring(request.uri().lastIndexOf('/') + 1))))))
      .prompt(new McpSchema.Prompt("greet", "a greeting", List.of(new McpSchema.PromptArgument("name",
          "who to greet", true))),
          (exchange, request) -> Future.succeededFuture(new McpSchema.GetPromptResult("a greeting",
              List.of(new McpSchema.PromptMessage(McpSchema.Role.USER,
                  new McpSchema.TextContent("Hello, " + request.arguments().get("name")))))))
      .build();
  }

  private static Future<McpSchema.CallToolResult> slow(McpServerExchange exchange,
      McpSchema.CallToolRequest request) {
    return exchange.progress(1, 3.0, "step 1")
      .compose(v -> exchange.progress(2, 3.0, "step 2"))
      .compose(v -> exchange.progress(3, 3.0, "step 3"))
      .map(v -> McpSchema.CallToolResult.builder().addTextContent("done").build());
  }

  private static Future<McpSchema.CallToolResult> ask(McpServerExchange exchange,
      McpSchema.CallToolRequest request) {
    McpSchema.CreateMessageRequest sampling = McpSchema.CreateMessageRequest.builder()
      .messages(List.of(new McpSchema.SamplingMessage(McpSchema.Role.USER,
          new McpSchema.TextContent(String.valueOf(request.arguments().get("question"))))))
      .systemPrompt("be terse")
      .maxTokens(64)
      .build();
    return exchange.createMessage(sampling)
      .map(result -> McpSchema.CallToolResult.builder()
        .addTextContent("model said: " + ((McpSchema.TextContent) result.content()).text())
        .build());
  }

  private static Future<McpSchema.CallToolResult> noisy(McpServerExchange exchange,
      McpSchema.CallToolRequest request) {
    return exchange.log(McpSchema.LoggingLevel.INFO, "noisy", "an info line")
      .compose(v -> exchange.log(McpSchema.LoggingLevel.ERROR, "noisy", "an error line"))
      .map(v -> McpSchema.CallToolResult.builder().addTextContent("noise made").build());
  }

  private static String textOf(McpSchema.CallToolResult result) {
    return ((McpSchema.TextContent) result.content().get(0)).text();
  }

  private static String textOf(McpSchema.ReadResourceResult result) {
    return ((McpSchema.TextResourceContents) result.contents().get(0)).text();
  }
}
