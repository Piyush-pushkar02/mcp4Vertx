---
title: Migrating from the official SDK
nav_order: 7
---

# Migrating from the official SDK

The main thing to know before you start: **`McpSchema` types are identical between the two SDKs.**
This project depends on `io.modelcontextprotocol.sdk:mcp-core` for exactly that package, with
`reactor-core` excluded. Your `McpSchema.Tool` declarations, your `CallToolResult` construction, your
`JsonSchema` objects, your `PromptMessage` trees — none of it changes. The same `import
io.modelcontextprotocol.spec.McpSchema;` line stays at the top of the file.

What changes is the *shape* of the async plumbing around them: `Mono<T>` becomes `Future<T>`, and
the builders and exchange objects have different names.

## Types

| Official SDK | mcp4vertx |
|---|---|
| `Mono<T>` | `io.vertx.core.Future<T>` |
| `Mono<Void>` | `Future<Void>` |
| `Flux<T>` | *(not used; the SDK has no streaming result type)* |
| `McpSchema.*` | **unchanged** — same package, same classes |
| `McpError` | **unchanged** |
| `io.modelcontextprotocol.json.TypeRef` | **unchanged** |
| `io.modelcontextprotocol.common.McpTransportContext` | `io.github.mcp4vertx.McpTransportContext` |

## Operator mapping

| Reactor | Vert.x `Future` |
|---|---|
| `Mono.just(x)` | `Future.succeededFuture(x)` |
| `Mono.error(e)` | `Future.failedFuture(e)` |
| `Mono.empty()` | `Future.succeededFuture()` |
| `.map(f)` | `.map(f)` |
| `.flatMap(f)` | `.compose(f)` |
| `.onErrorResume(f)` | `.recover(f)` |
| `.onErrorReturn(x)` | `.otherwise(x)` |
| `.doOnSuccess(c)` | `.onSuccess(c)` |
| `.doOnError(c)` | `.onFailure(c)` |
| `.doFinally(c)` | `.eventually(supplier)` or `.andThen(c)` |
| `.then()` | `.mapEmpty()` |
| `Mono.zip(a, b)` | `Future.all(a, b)` |
| `Mono.fromCallable(c).subscribeOn(boundedElastic())` | `vertx.executeBlocking(c)` |
| `.block()` | *nothing — compose futures instead* |

A `Future` is eager: it represents work already started, not a recipe to be subscribed to. In
practice that removes a class of bug (a `Mono` nobody subscribed to does nothing), and means you
never need a `.subscribe()` at the end of a chain.

## Server

| Official SDK | mcp4vertx |
|---|---|
| `McpServer.async(provider)` | `VertxMcpServer.builder(vertx)` |
| `McpServer.sync(provider)` | `VertxMcpServer.builder(vertx)` — one builder, no sync/async split |
| `.serverInfo(name, version)` | `.serverInfo(name, version)` |
| `.instructions(text)` | `.instructions(text)` |
| `.capabilities(caps)` | `.capabilities(caps)` |
| `.requestTimeout(duration)` | `.requestTimeout(duration)` |
| `.uriTemplateManagerFactory(f)` | `.uriTemplateFactory(f)` |
| `.jsonSchemaValidator(v)` | `.structuredOutputValidator(v)` |
| `.jsonMapper(m)` | `.codec(new JsonRpcCodec(m))` |
| `.tools(new AsyncToolSpecification(tool, handler))` | `.tool(tool, handler)` |
| `.tools(new SyncToolSpecification(tool, handler))` | `.toolBlocking(tool, handler)` |
| `.toolCall(tool, handler)` | `.tool(tool, handler)` |
| `.resources(new AsyncResourceSpecification(r, h))` | `.resource(r, h)` |
| `.resources(new SyncResourceSpecification(r, h))` | `.resourceBlocking(r, h)` |
| `.resourceTemplates(new AsyncResourceTemplateSpecification(t, h))` | `.resourceTemplate(t, h)` |
| `.prompts(new AsyncPromptSpecification(p, h))` | `.prompt(p, h)` |
| `.prompts(new SyncPromptSpecification(p, h))` | `.promptBlocking(p, h)` |
| `.completions(new AsyncCompletionSpecification(ref, h))` | `.completion(ref, h)` |
| `.build()` → `McpAsyncServer` | `.build()` → `VertxMcpServer` |
| `McpSyncServer` + `.block()` everywhere | there is no sync server — compose `Future`s |
| `server.addTool(new AsyncToolSpecification(t, h))` | `server.addTool(t, h)` |
| `server.removeTool(name)` | `server.removeTool(name)` |
| `server.addResource(spec)` / `removeResource(uri)` | `server.addResource(r, h)` / `removeResource(uri)` |
| `server.addPrompt(spec)` / `removePrompt(name)` | `server.addPrompt(p, h)` / `removePrompt(name)` |
| `server.notifyToolsListChanged()` | same |
| `server.notifyResourcesListChanged()` | same |
| `server.notifyPromptsListChanged()` | same |
| `server.notifyResourcesUpdated(notification)` | `server.notifyResourceUpdated(uri)` |
| `server.closeGracefully()` | close the transports; `transport.close()` |

Side by side. Official:

```java
McpAsyncServer server = McpServer.async(provider)
    .serverInfo("weather", "1.0.0")
    .instructions("Ask for a forecast by city.")
    .tools(new McpServerFeatures.AsyncToolSpecification(forecastTool,
        (exchange, request) -> Mono.fromCallable(() -> forecastFor(request))
            .flatMap(text -> exchange.progressNotification(progress).thenReturn(text))
            .map(text -> McpSchema.CallToolResult.builder().addTextContent(text).build())))
    .build();
```

mcp4vertx:

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .serverInfo("weather", "1.0.0")
    .instructions("Ask for a forecast by city.")
    .tool(forecastTool, (exchange, request) -> exchange.progress(0.5, 1.0, "calling upstream")
        .map(v -> McpSchema.CallToolResult.builder()
            .addTextContent(forecastFor(request))
            .build()))
    .build();
```

Note the tool declaration (`forecastTool`) and the result construction are byte-identical. Only the
async glue moved.

### The sync/async split disappears

Upstream has two parallel APIs — `McpAsyncServer` with `AsyncToolSpecification`, and `McpSyncServer`
with `SyncToolSpecification` wrapping the async one and blocking on it. This SDK has one server, and
the sync/async choice is per handler:

```java
VertxMcpServer.builder(vertx)
    .tool(asyncTool, (exchange, request) -> webClient.get(...).send().map(this::toResult))
    .toolBlocking(jdbcTool, (exchange, request) -> dao.query(request))   // executeBlocking
    .build();
```

A `SyncToolSpecification` becomes `toolBlocking`. That is a closer match than it looks: upstream's
sync server also dispatches sync handlers off the reactive thread, and this does the same thing with
`vertx.executeBlocking`. The difference is that here it is visible at the call site.

If you were calling `.block()` on the async API to avoid the sync wrapper, delete the `.block()` and
compose the future instead.

## Exchange

| Official SDK | mcp4vertx |
|---|---|
| `McpAsyncServerExchange` | `McpServerExchange` |
| `McpSyncServerExchange` | `McpServerExchange` (no separate sync type) |
| `exchange.getClientCapabilities()` | `exchange.clientCapabilities()` |
| `exchange.getClientInfo()` | `exchange.clientInfo()` |
| `exchange.sessionId()` | `exchange.sessionId()` |
| `exchange.transportContext()` | `exchange.transportContext()` |
| `Mono<CreateMessageResult> createMessage(req)` | `Future<CreateMessageResult> createMessage(req)` |
| `Mono<ElicitResult> createElicitation(req)` | `Future<ElicitResult> createElicitation(req)` |
| `Mono<ListRootsResult> listRoots()` / `listRoots(cursor)` | same, returning `Future` |
| `Mono<Void> loggingNotification(n)` | `Future<Void> log(n)`, plus `log(level, logger, message)` |
| `Mono<Void> progressNotification(n)` | `Future<Void> progress(n)`, plus `progress(progress, total, message)` |
| `Mono<Object> ping()` | `Future<Void> ping()` |

The two convenience overloads are worth adopting. `progress(double, Double, String)` is a no-op when
the client sent no progress token, so you can drop the null check you were doing; and `log(level,
logger, message)` is filtered against the client's `logging/setLevel` without a round trip.

## Server transports

| Official SDK | mcp4vertx |
|---|---|
| `HttpServletStreamableServerTransportProvider` | `StreamableHttpServerTransport` |
| `HttpServletSseServerTransportProvider` | `SseServerTransport` |
| `HttpServletStatelessServerTransport` | `StatelessHttpServerTransport` |
| `StdioServerTransportProvider` | `StdioServerTransport` |
| `WebFluxStreamableServerTransportProvider` (spring module) | `StreamableHttpServerTransport` |
| `WebMvcStreamableServerTransportProvider` (spring module) | `StreamableHttpServerTransport` |

The mounting model changes. Upstream's servlet providers are `HttpServlet`s you register in a
servlet container:

```java
var provider = HttpServletStreamableServerTransportProvider.builder().mcpEndpoint("/mcp").build();
context.addServlet(new ServletHolder(provider), "/*");
```

Here they hand you a vertx-web `Router` to mount:

```java
Router app = Router.router(vertx);
app.route("/mcp*").handler(JWTAuthHandler.create(jwtAuth));
app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());
vertx.createHttpServer().requestHandler(app).listen(8080);
```

Wiring is also reversed. Upstream builds the transport provider first and hands it to
`McpServer.async(provider)`. Here you build the server first and hand it to the transport, which
takes the server's session factory and codec and registers itself as a broadcaster:

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)./* features */.build();
StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, server);
```

That is what lets one server serve several transports at once.

Transport options map roughly one to one: `mcpEndpoint`, `keepAliveInterval`, and
`contextExtractor` keep their names. Upstream's `ServerTransportSecurityValidator` corresponds to
`allowedOrigins(...)` here.

## Client

| Official SDK | mcp4vertx |
|---|---|
| `McpClient.async(transport)` | `VertxMcpClient.builder(vertx)` |
| `McpClient.sync(transport)` | `VertxMcpClient.builder(vertx)` — no sync client |
| `.clientInfo(impl)` | `.clientInfo(impl)` or `.clientInfo(name, version)` |
| `.requestTimeout(d)` / `.initializationTimeout(d)` | `.requestTimeout(d)` |
| `.sampling(handler)` | `.sampling(handler)` — returns `Future`, not `Mono` |
| `.elicitation(handler)` | `.elicitation(handler)` |
| `.roots(...)` | `.roots(List)` / `.root(Root)` |
| `.toolsChangeConsumer(c)` | `.toolsChangeHandler(() -> ...)` — no payload, re-list |
| `.resourcesChangeConsumer(c)` | `.resourcesChangeHandler(() -> ...)` |
| `.promptsChangeConsumer(c)` | `.promptsChangeHandler(() -> ...)` |
| `.resourcesUpdateConsumer(c)` | `.resourceUpdatedHandler(uri -> ...)` |
| `.loggingConsumer(c)` | `.loggingHandler(n -> ...)` |
| `.progressConsumer(c)` | `.progressHandler(n -> ...)` |
| `.jsonSchemaValidator(v)` | *(client-side validation is not performed)* |
| `client.initialize()` | `client.initialize()` |
| `client.listTools()` / `listTools(cursor)` | same |
| — | `client.listAllTools()` — walks `nextCursor` for you |
| `client.callTool(req)` | same |
| `client.listResources()` / `readResource(...)` | same, plus `listAllResources()` |
| `client.listResourceTemplates()` | same, plus `listAllResourceTemplates()` |
| `client.subscribeResource(new SubscribeRequest(uri))` | `client.subscribeResource(uri)` |
| `client.unsubscribeResource(new UnsubscribeRequest(uri))` | `client.unsubscribeResource(uri)` |
| `client.listPrompts()` / `getPrompt(req)` | same, plus `listAllPrompts()` |
| `client.completeCompletion(req)` | same |
| `client.setLoggingLevel(level)` | same |
| `client.addRoot(root)` / `removeRoot(uri)` | same |
| `client.closeGracefully()` | `client.close()` |

## Client transports

| Official SDK | mcp4vertx |
|---|---|
| `HttpClientStreamableHttpTransport.builder(url)` | `StreamableHttpClientTransport.builder(vertx).baseUrl(url)` |
| `HttpClientSseClientTransport.builder(url)` | `SseClientTransport.builder(vertx).baseUrl(url)` |
| `StdioClientTransport(ServerParameters)` | `StdioClientTransport.builder(vertx).command(...).args(...)` |
| `WebClientStreamableHttpTransport` (spring module) | `StreamableHttpClientTransport` |
| `McpSyncHttpClientRequestCustomizer` | `.requestCustomizer(Function<HttpClientRequest, HttpClientRequest>)` |

The customizer receives a vertx-core `HttpClientRequest`, so `bearerTokenAuthentication(token)`
becomes `putHeader("Authorization", "Bearer " + token)`.

## Testing

Upstream's sync client blocks, so its tests run on a plain JUnit thread. Here, drive the future
chain and let `VertxTestContext` decide when the test is done:

```java
@ExtendWith(VertxExtension.class)
class MyTest {

  @Test
  void callsATool(Vertx vertx, VertxTestContext testContext) {
    client.initialize()
        .compose(init -> client.callTool(new McpSchema.CallToolRequest("echo", Map.of("text", "hi"))))
        .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
          assertThat(((McpSchema.TextContent) result.content().get(0)).text()).isEqualTo("hi");
          testContext.completeNow();
        })));
  }
}
```

From a plain thread — a `main` method, or a test that also drives a blocking upstream component —
bridge out explicitly:

```java
McpSchema.InitializeResult init = client.initialize()
    .toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
```

## One classpath caveat

If you keep both SDKs on the same classpath — for a migration period, or to run cross-SDK tests —
be aware that upstream's default `JsonSchemaValidator` needs `com.networknt:json-schema-validator`
3.x, which shares its package with the 1.x this project compiles against. Only one can be present.
This repository's own conformance tests hand the upstream client and server a permissive validator
for exactly this reason; the server under test does its own output-schema validation regardless.

If you are migrating rather than running both, this does not arise: drop the upstream server and
client artifacts and keep only `mcp-core`, which is what `mcp4vertx-core` already depends on.
