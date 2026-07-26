---
title: Building a server
nav_order: 2
---

# Building a server

`VertxMcpServer` is a registry of tools, resources, and prompts plus the JSON-RPC routing that
exposes them. It is transport-agnostic: build it once, then attach one or more transports to it.

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .serverInfo("weather", "1.0.0")
    .instructions("Ask for a forecast by city.")
    .tool(forecastTool, this::forecast)
    .build();
```

## Builder options

| Method | Default | Meaning |
|---|---|---|
| `serverInfo(String name, String version)` | `mcp4vertx-server 0.1.0` | Identity reported at `initialize` |
| `serverInfo(McpSchema.Implementation)` | | Same, pre-built |
| `instructions(String)` | `null` | Free text describing the server to a model |
| `capabilities(McpSchema.ServerCapabilities)` | derived | Override the advertised capabilities |
| `requestTimeout(Duration)` | 30s | How long to wait for a client's answer to a server-initiated request |
| `pageSize(int)` | `0` | Items per page on list operations. Zero returns everything in one page |
| `eventStore(EventStore)` | `InMemoryEventStore` | Where SSE events are recorded for resumption |
| `structuredOutputValidator(McpStructuredOutputValidator)` | auto-detected | Validates `structuredContent` against `outputSchema` |
| `uriTemplateFactory(McpUriTemplateManagerFactory)` | `DefaultMcpUriTemplateManagerFactory` | RFC 6570 template engine |
| `codec(JsonRpcCodec)` | Jackson 2 | JSON-RPC serialization |

## Tools

### Async tools

A tool handler is `(McpServerExchange, McpSchema.CallToolRequest) -> Future<McpSchema.CallToolResult>`.

```java
McpSchema.Tool forecast = McpSchema.Tool.builder()
    .name("forecast")
    .description("Multi-day forecast for a city. Reports progress while it works.")
    .inputSchema(new McpSchema.JsonSchema("object",
        Map.of("city", Map.of("type", "string")), List.of("city"), false, null, null))
    .build();

builder.tool(forecast, (exchange, request) -> {
  String city = String.valueOf(request.arguments().get("city"));
  return exchange.progress(0.0, 3.0, "resolving " + city)
      .compose(v -> exchange.progress(1.0, 3.0, "querying stations"))
      .compose(v -> exchange.progress(2.0, 3.0, "rendering"))
      .compose(v -> exchange.log(McpSchema.LoggingLevel.INFO, "forecast", "ready"))
      .map(v -> McpSchema.CallToolResult.builder()
          .addTextContent(city + ": 18C, light rain, wind 12 kph")
          .build());
});
```

Failing the future produces a JSON-RPC error. Returning a result with `isError(true)` reports
failure in band, which is what the MCP spec wants for a tool that ran but did not succeed:

```java
// JSON-RPC error: the tool itself broke.
builder.tool(explode, (exchange, request) ->
    Future.failedFuture(new IllegalStateException("detonated")));

// In-band failure: the tool worked, the answer is "no".
builder.tool(refuse, (exchange, request) ->
    Future.succeededFuture(McpSchema.CallToolResult.builder()
        .addTextContent("not today")
        .isError(true)
        .build()));
```

Anything thrown synchronously from a handler is also caught and turned into a JSON-RPC error, so a
handler that trips a `NullPointerException` fails the one call rather than the session.

### Blocking tools

`toolBlocking` takes a handler that returns a `CallToolResult` directly and may throw. It is run
via `vertx.executeBlocking`, so the event loop is never parked, and the resulting future completes
back on the calling context.

```java
builder.toolBlocking(windChillTool, (exchange, request) -> {
  double celsius = ((Number) request.arguments().get("celsius")).doubleValue();
  double windKph = ((Number) request.arguments().get("windKph")).doubleValue();
  double v = Math.pow(windKph, 0.16);
  double chill = 13.12 + 0.6215 * celsius - 11.37 * v + 0.3965 * celsius * v;
  return McpSchema.CallToolResult.builder().addTextContent(String.format("%.1f", chill)).build();
});
```

`resourceBlocking` and `promptBlocking` exist with the same shape. There is no blocking variant for
completions — those are meant to be fast.

Blocking is explicit and never accidental. See [Threading model](threading.md) for the full
discussion, including virtual threads.

### Structured output and `outputSchema`

Declare an `outputSchema` on the tool and populate `structuredContent` on the result:

```java
Map<String, Object> totalSchema = Map.of(
    "type", "object",
    "properties", Map.of("total", Map.of("type", "number")),
    "required", List.of("total"));

builder.tool(
    McpSchema.Tool.builder().name("sum").description("adds two numbers").outputSchema(totalSchema).build(),
    (exchange, request) -> Future.succeededFuture(McpSchema.CallToolResult.builder()
        .addTextContent("sum computed")
        .structuredContent(Map.of("total",
            ((Number) request.arguments().get("a")).intValue()
                + ((Number) request.arguments().get("b")).intValue()))
        .build()));
```

When a tool declares an `outputSchema`, the server validates the result's `structuredContent`
against it before the result goes on the wire. A violation fails the call with
`INTERNAL_ERROR` and a message containing `violates its outputSchema` — the client sees a clear
server-side bug report rather than data its own schema check will reject later.

Validation is skipped when the result has `isError(true)`, since an error result is not required to
match the success schema.

**The validator is an optional dependency.** `com.networknt:json-schema-validator` is declared
`<optional>true</optional>` in `mcp4vertx-core`:

- Present on the classpath: `McpStructuredOutputValidator.defaultValidator()` resolves to the
  networknt-backed implementation and output is validated.
- Absent: it resolves to `McpStructuredOutputValidator.permissive()` and validation is silently
  skipped. Nothing fails; you simply do not get the check.

Check which mode you are in with `McpStructuredOutputValidator.available()`, and override
explicitly with `builder.structuredOutputValidator(...)` if you want your own.

## Resources

An exact-URI resource:

```java
builder.resource(
    McpSchema.Resource.builder()
        .uri("weather://stations")
        .name("stations")
        .description("Every station this server can report on")
        .mimeType("text/plain")
        .build(),
    (exchange, request) -> Future.succeededFuture(new McpSchema.ReadResourceResult(
        List.of(new McpSchema.TextResourceContents("weather://stations", "text/plain",
            "LFPG\nEGLL\nKJFK")))));
```

### Resource templates (RFC 6570)

A template matches a family of URIs. `resources/read` first looks for an exact match, then walks
the templates in registration order and dispatches to the first whose `uriTemplate` matches. The
handler receives the concrete URI the client asked for, so you extract variables yourself:

```java
static final String FORECAST_TEMPLATE = "weather://forecast/{city}";

builder.resourceTemplate(
    McpSchema.ResourceTemplate.builder()
        .uriTemplate(FORECAST_TEMPLATE)
        .name("forecast")
        .description("The forecast document for one city")
        .mimeType("text/plain")
        .build(),
    (exchange, request) -> {
      Map<String, String> variables = new DefaultMcpUriTemplateManagerFactory()
          .create(FORECAST_TEMPLATE)
          .extractVariableValues(request.uri());
      String city = variables.getOrDefault("city", "nowhere");
      return Future.succeededFuture(new McpSchema.ReadResourceResult(
          List.of(new McpSchema.TextResourceContents(request.uri(), "text/plain",
              city + ": 18C, light rain"))));
    });
```

Hoist the `McpUriTemplateManager` into a static field in real code; it is created per call above
only to keep the sample self-contained.

A URI matching neither an exact resource nor any template fails with MCP's
`RESOURCE_NOT_FOUND` error code.

{: .warning }
**Per-resource subscriptions are not implemented.** The derived capabilities advertise
`resources.subscribe = false`, and no handler is registered for `resources/subscribe` or
`resources/unsubscribe`, so a client that calls them gets `METHOD_NOT_FOUND`. That is consistent —
the server never claims a capability it does not have — but it means `notifyResourceUpdated(uri)`
below is a broadcast to every connected session rather than a targeted delivery to subscribers. If
you need real subscriptions, keep the subscriber set yourself and filter on the client side, or
override `capabilities(...)` and register the handlers through your own routing.

## Prompts

```java
builder.prompt(
    new McpSchema.Prompt("travel-advice", "Should I pack a coat?",
        List.of(new McpSchema.PromptArgument("city", "Where you are going", true))),
    (exchange, request) -> {
      Object city = request.arguments() == null ? null : request.arguments().get("city");
      return Future.succeededFuture(new McpSchema.GetPromptResult("Packing advice",
          List.of(new McpSchema.PromptMessage(McpSchema.Role.USER,
              new McpSchema.TextContent("I am travelling to " + city + ". What should I pack?")))));
    });
```

## Completions

A completion handler is registered against a `CompleteReference` — either a prompt by name or a
resource template by URI.

```java
builder.completion(
    new McpSchema.PromptReference("travel-advice"),
    (exchange, request) -> {
      String prefix = request.argument().value();
      List<String> matches = Stream.of("Paris", "Prague", "Porto")
          .filter(city -> city.startsWith(prefix))
          .toList();
      return Future.succeededFuture(new McpSchema.CompleteResult(
          new McpSchema.CompleteResult.CompleteCompletion(matches, matches.size(), false)));
    });

builder.completion(
    new McpSchema.ResourceReference("weather://forecast/{city}"),
    (exchange, request) -> ...);
```

A `completion/complete` for a reference with no registered handler is answered with an empty
completion rather than an error. That is deliberate: completions are a convenience, and a client
asking about something the server does not complete should get "nothing to suggest", not a failure.

## Capabilities

By default, capabilities are **derived from what you register**:

| Capability | Advertised when |
|---|---|
| `tools` (with `listChanged`) | at least one tool is registered |
| `resources` (`subscribe=false`, `listChanged=true`) | at least one resource or resource template is registered |
| `prompts` (with `listChanged`) | at least one prompt is registered |
| `completions` | at least one completion handler is registered |
| `logging` | always |

Handlers are only wired for capabilities that are advertised, so a server with no prompts answers
`prompts/list` with `METHOD_NOT_FOUND` rather than an empty list. If you need a different shape,
pass your own:

```java
builder.capabilities(new McpSchema.ServerCapabilities(
    null, null,
    new McpSchema.ServerCapabilities.LoggingCapabilities(),
    new McpSchema.ServerCapabilities.PromptCapabilities(true),
    new McpSchema.ServerCapabilities.ResourceCapabilities(false, true),
    new McpSchema.ServerCapabilities.ToolCapabilities(true)));
```

Overriding capabilities decides which handlers get registered, so advertising something you have
not registered a handler for will produce errors at call time, not at build time.

## Talking back to the client mid-request

`McpServerExchange` is what a handler is handed. On Streamable HTTP the underlying session is the
*per-request stream*, so everything emitted here is written onto the same SSE response the eventual
result goes to.

```java
public final class McpServerExchange {
  String sessionId();
  McpSchema.ClientCapabilities clientCapabilities();
  McpSchema.Implementation clientInfo();
  McpTransportContext transportContext();
  Object progressToken();
  McpSession session();

  Future<McpSchema.CreateMessageResult> createMessage(McpSchema.CreateMessageRequest request);
  Future<McpSchema.ElicitResult> createElicitation(McpSchema.ElicitRequest request);
  Future<McpSchema.ListRootsResult> listRoots();
  Future<McpSchema.ListRootsResult> listRoots(String cursor);
  Future<Void> ping();

  Future<Void> log(McpSchema.LoggingMessageNotification notification);
  Future<Void> log(McpSchema.LoggingLevel level, String logger, String message);
  Future<Void> progress(McpSchema.ProgressNotification notification);
  Future<Void> progress(double progress, Double total, String message);
}
```

### Progress

`progress(double, Double, String)` is a no-op when the client supplied no `progressToken`, so call
it unconditionally:

```java
return exchange.progress(1, 3.0, "step 1")
    .compose(v -> exchange.progress(2, 3.0, "step 2"))
    .compose(v -> exchange.progress(3, 3.0, "step 3"))
    .map(v -> McpSchema.CallToolResult.builder().addTextContent("done").build());
```

### Logging

`log(...)` is filtered against the level the client last asked for via `logging/setLevel`, without a
round trip. The default minimum is `INFO`.

```java
return exchange.log(McpSchema.LoggingLevel.INFO, "noisy", "an info line")
    .compose(v -> exchange.log(McpSchema.LoggingLevel.ERROR, "noisy", "an error line"))
    .map(v -> McpSchema.CallToolResult.builder().addTextContent("noise made").build());
```

### Sampling from inside a tool

The server can ask the client's model to complete something, so the server never needs model
credentials:

```java
builder.tool(askTool, (exchange, request) -> {
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
});
```

`createMessage` fails fast with `INVALID_REQUEST` and "Client does not support sampling" if the
client did not declare the capability at `initialize`. `createElicitation` does the same for
elicitation. On the stateless transport, where there is no session to carry a reverse request, both
fail with `UnsupportedOperationException` rather than silently succeeding.

### Transport metadata

`exchange.transportContext()` carries whatever the transport's context extractor put there. The
default extractor stashes every request header plus the `RoutingContext` itself:

```java
Map<?, ?> headers = exchange.transportContext()
    .get(HttpTransports.HEADERS_KEY, Map.class)
    .orElse(Map.of());
Object authorization = headers.get("authorization");

RoutingContext rc = exchange.transportContext()
    .get(HttpTransports.ROUTING_CONTEXT_KEY, RoutingContext.class)
    .orElseThrow();
User user = rc.user();
```

Supply your own extractor to narrow that down — see [Transports](transports.md).

The context is passed explicitly, not smuggled through a thread-local or a subscriber context. On
the stateless transport it is the only channel a handler has to the caller's identity, since there
is no session.

## Dynamic registry and list-changed notifications

Tools, resources, and prompts can be added and removed at runtime. Each mutation returns a `Future`
that completes when the corresponding `list_changed` notification has been broadcast to every
connected client on every registered transport.

```java
server.addTool(lateArrival, (exchange, request) -> ...);   // Future<Void>
server.removeTool("late-arrival");
server.addResource(resource, handler);
server.removeResource("weather://stations");
server.addPrompt(prompt, handler);
server.removePrompt("travel-advice");
server.addResourceTemplate(template, handler);
server.removeResourceTemplate("weather://forecast/{city}");
```

`removeX` for something that was never registered is a no-op that completes successfully and sends
no notification.

Templates are matched in registration order, so add a more specific template before a more general
one that would also match.

You can also announce a change explicitly:

```java
server.notifyToolsListChanged();
server.notifyResourcesListChanged();
server.notifyPromptsListChanged();
server.notifyResourceUpdated("weather://stations");
```

A broadcast is best-effort per transport: a failure on one transport is logged at debug and does not
fail the others. That is deliberate — a session whose client has no listening stream open cannot
receive unsolicited traffic, which is normal, not an error. On Streamable HTTP those notifications
are still recorded in the event store so a reconnecting client can replay them; see
[Resumability](resumability.md).

The registry is backed by concurrent collections, so mutating it from any thread is safe even while
sessions on other event loops are serving `tools/list`. Listings are sorted by name (by URI for
resources), so a client sees a stable order regardless of registration order or of which node
answered.

{: .note }
Offset-based pagination interacts poorly with a registry that changes mid-walk: adding or removing
an entry shifts the window, so a client paging through can see an item twice or miss one. If you
mutate the registry frequently, leave pagination off.

## Pagination

Pagination is off by default: `pageSize(0)` means every list operation returns everything in one
page with no `nextCursor`. That is the right default for the tens-of-items registries most servers
have, and it means a naive client that ignores `nextCursor` cannot silently miss entries.

Turn it on with a positive page size:

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .pageSize(50)
    .tool(...)
    .build();
```

Cursors are opaque to clients: base64url of the offset. A malformed cursor is rejected with
`INVALID_PARAMS`. It applies uniformly to `tools/list`, `resources/list`,
`resources/templates/list`, and `prompts/list`.

The trade-off worth naming: because cursors are offsets into a list, adding or removing entries
while a client is mid-walk shifts the window. For a registry that changes at runtime, prefer
leaving pagination off and relying on `list_changed` for invalidation.

On the client side, `listAllTools()` and its siblings walk `nextCursor` for you — see
[Client](client.md).

## Attaching transports

A server is not bound to a transport until you create one against it, and a single server can serve
several at once:

```java
VertxMcpServer server = buildMcpServer(vertx);

Router app = Router.router(vertx);
app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());
app.route("/legacy/*").subRouter(SseServerTransport.create(vertx, server).router());
```

Each transport registers itself as a broadcaster on the server when built, so `list_changed`
notifications reach clients on every one of them. Full details in [Transports](transports.md).
