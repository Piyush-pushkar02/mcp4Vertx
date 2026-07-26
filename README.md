# vertx-mcp

A **Vert.x-native SDK for the Model Context Protocol**. Everything returns
`io.vertx.core.Future`. There is no Reactor anywhere on the compile classpath, no scheduler
indirection, and no thread hopping you did not ask for.

```xml
<dependency>
  <groupId>io.vertx.mcp</groupId>
  <artifactId>vertx-mcp-server</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Why this exists rather than wrapping the official SDK

The official `io.modelcontextprotocol.sdk:mcp` artifact is an empty aggregator; the code is
21.4k lines in `mcp-core`. It is not Jetty-coupled — `jakarta.servlet` appears in exactly
four files, all under `server/transport/`, at `provided` scope. What it *is* coupled to is
**Reactor**, and that is the part that does not fit Vert.x.

Reactor has no notion of a Vert.x `Context`. Any hop to `boundedElastic`, or any `Mono`
completed by a non-Vert.x thread, leaves the continuation on a thread where
`Vertx.currentContext()` returns `null` — which silently breaks context-local storage,
tracing propagation, and ordered task execution. You can paper over it with a custom
`Scheduler` and a `Mono`↔`Future` bridge, but it leaks: one
`Mono.fromCallable(..).subscribeOn(boundedElastic())` in a user's tool handler walks
straight past the bridge.

So this SDK reuses exactly one thing from upstream — `McpSchema`, the 2,786 lines of wire
DTOs, which import zero Reactor classes — and implements sessions, lifecycle, routing and
every transport natively:

```xml
<dependency>
  <groupId>io.modelcontextprotocol.sdk</groupId>
  <artifactId>mcp-core</artifactId>
  <exclusions>
    <exclusion><groupId>io.projectreactor</groupId><artifactId>reactor-core</artifactId></exclusion>
    <exclusion><groupId>jakarta.servlet</groupId><artifactId>jakarta.servlet-api</artifactId></exclusion>
  </exclusions>
</dependency>
```

Protocol revisions churn the DTOs; that stays upstream's problem. The concurrency model is
ours.

## Threading model

Every session is **pinned to the Vert.x context it was created on**. All mutable session
state — pending-request maps, stream registries, logging levels — is touched only from that
one thread, so it is plain `HashMap`/`HashSet` with no locks, and message ordering within a
session is deterministic. Calls arriving on another event loop hop via
`Contexts.on(...)`. Handlers therefore always run with a live `Vertx.currentContext()`.

Blocking handlers are explicit, never accidental:

```java
.tool(asyncTool, (exchange, request) -> webClient.get(...).send().map(this::toResult))
.toolBlocking(jdbcTool, (exchange, request) -> dao.query(request))   // executeBlocking
```

## Server

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .serverInfo("weather", "1.0.0")
    .instructions("Weather data for planning trips.")
    .tool(forecastTool, (exchange, request) -> {
      exchange.progress(0.5, 1.0, "calling upstream");        // rides the same SSE response
      return upstream.forecast(request.arguments().get("city"))
          .map(VertxMcpServer::asResult);
    })
    .resource(stationsResource, (exchange, request) -> ...)
    .resourceTemplate(forecastTemplate, (exchange, request) -> ...)   // RFC 6570
    .prompt(travelAdvice, (exchange, request) -> ...)
    .build();
```

Mount it into an existing app router — it composes with your auth handlers, CORS and body
limits rather than owning the port:

```java
Router app = Router.router(vertx);
app.get("/health").handler(rc -> rc.response().end("ok"));
app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());
vertx.createHttpServer().requestHandler(app).listen(8080);
```

### Transports

| Transport | Class | Protocol |
|---|---|---|
| Streamable HTTP | `StreamableHttpServerTransport` | 2025-03-26 → 2025-11-25 |
| HTTP+SSE (legacy) | `SseServerTransport` | 2024-11-05 |
| Stateless HTTP | `StatelessHttpServerTransport` | no sessions, no SSE |
| stdio | `StdioServerTransport` | local subprocess |

Streamable HTTP implements: `POST` with JSON-vs-SSE negotiation, `GET` for the standalone
listening stream, `DELETE` for session teardown, `Mcp-Session-Id`, `MCP-Protocol-Version`
validation, `Last-Event-ID` resumption, `Origin` allow-listing for DNS-rebinding protection,
and SSE keep-alive comments.

### Resumability actually works

Upstream's `McpStreamableServerSession.replay()` returns `Flux.empty()` — resumability is
declared but not implemented. Here it is backed by a pluggable `EventStore`
(`InMemoryEventStore` by default, bounded per stream). Two properties the tests pin down:

- Notifications emitted while the client's `GET` is **down** are still recorded, so a
  reconnect with `Last-Event-ID` replays them. Dropping them is how `tools/list_changed`
  goes missing across a reconnect — silently, because broadcast callers swallow send
  failures.
- Recorded events are purged when a response stream completes and when a session ends, so a
  long-lived server does not leak proportionally to total requests served.

Swap in Redis or a clustered `AsyncMap` for multi-node deployments.

## Client

```java
VertxMcpClient client = VertxMcpClient.builder(vertx)
    .transport(StreamableHttpClientTransport.builder(vertx, "http://localhost:8080").build())
    .clientInfo("my-app", "1.0.0")
    .sampling(request -> llm.complete(request))       // server -> client
    .elicitation(request -> ui.ask(request))
    .toolsChangeHandler(() -> refreshToolCache())
    .build();

client.initialize()
    .compose(init -> client.listAllTools())           // walks nextCursor for you
    .compose(tools -> client.callTool(new CallToolRequest("forecast", Map.of("city", "Pune"))))
    .onSuccess(result -> log.info("{}", result.content()));
```

Built on vertx-core's `HttpClient` — no `vertx-web-client`, and no internal Vert.x APIs. The
SSE read path is demand-driven: one chunk fetched at a time, withheld until the consumer has
taken every event, so a slow consumer stops the socket instead of growing a queue.

## Conformance

The suite does not only test this SDK against itself. It runs **both directions** against the
official SDK:

- the official `McpClient.sync(HttpClientStreamableHttpTransport...)` and
  `HttpClientSseClientTransport` against **our** server;
- **our** `VertxMcpClient` against the official
  `HttpServletStreamableServerTransportProvider` hosted in Jetty 12.

77 tests total, covering the initialize handshake, tools/resources/prompts, structured-output
schema validation (including a deliberate violation), progress notifications arriving on the
same POST stream before the result, a server-initiated `sampling/createMessage` from inside a
tool handler, `logging/setLevel` filtering, list-changed broadcasts, session teardown, 20
concurrent tool calls not cross-delivering, `Last-Event-ID` resumption over a killed socket,
and an SSE parser fuzzed at every possible chunk-split position.

```
mvn verify
```

## Modules

| Module | Lines | Contents |
|---|---|---|
| `vertx-mcp-core` | 1.8k | JSON-RPC codec, sessions, transport SPI, event store |
| `vertx-mcp-server` | 2.5k | Server, feature registry, four server transports |
| `vertx-mcp-client` | 2.6k | Client and three client transports |
| `vertx-mcp-examples` | 0.3k | Runnable examples + the conformance suite |

Java 17, Vert.x 5.1.5. Jackson 2 only — the upstream `mcp-json-jackson3` module is
deliberately not used, so you do not end up with two Jackson majors on the classpath.
`com.networknt:json-schema-validator` is **optional**: present, structured tool output is
validated against its `outputSchema`; absent, validation is skipped.

## Known gaps

- Pagination is offset-based and opt-in (`pageSize`, default: everything in one page).
- `InMemoryEventStore` is single-node; horizontal scaling needs a shared implementation.
- OAuth / authorization is out of scope — mount your own vertx-web auth handler in front of
  the sub-router.
