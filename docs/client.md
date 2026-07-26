---
title: Client
nav_order: 4
---

# Client

`VertxMcpClient` is the client end of the protocol: the handshake, the request methods a client can
send, and the handlers for everything the server sends back unprompted. Every method returns a
`Future` that completes on the Vert.x context the client was built on.

```java
VertxMcpClient client = VertxMcpClient.builder(vertx)
    .transport(StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:8080")
        .endpoint("/mcp")
        .build())
    .clientInfo("my-app", "1.0.0")
    .sampling(request -> llm.complete(request))
    .elicitation(request -> ui.ask(request))
    .progressHandler(progress -> logger.info("{}", progress.message()))
    .toolsChangeHandler(() -> refreshToolCache())
    .build();

client.initialize()
    .compose(init -> client.listAllTools())
    .compose(tools -> client.callTool(new McpSchema.CallToolRequest("forecast", Map.of("city", "Pune"))))
    .onSuccess(result -> logger.info("{}", result.content()));
```

## Builder options

| Method | Default | Meaning |
|---|---|---|
| `transport(McpClientTransport)` | required | See [Transports](transports.md) |
| `clientInfo(String name, String version)` | `mcp4vertx-client 0.1.0` | Identity sent at `initialize` |
| `capabilities(McpSchema.ClientCapabilities)` | derived | Override what is advertised |
| `requestTimeout(Duration)` | 30s | Deadline for any single request |
| `sessionId(String)` | random 8 chars | Prefix for generated JSON-RPC request ids |
| `codec(JsonRpcCodec)` | Jackson 2 | JSON-RPC serialization |
| `roots(List<Root>)` / `root(Root)` | none | Answered to `roots/list` |
| `sampling(SamplingHandler)` | none | Answers `sampling/createMessage`; declares the capability |
| `elicitation(ElicitationHandler)` | none | Answers `elicitation/create`; declares the capability |
| `loggingHandler(LoggingHandler)` | none | Receives `notifications/message` |
| `progressHandler(ProgressHandler)` | none | Receives `notifications/progress` |
| `toolsChangeHandler(ListChangedHandler)` | none | Receives `notifications/tools/list_changed` |
| `resourcesChangeHandler(ListChangedHandler)` | none | |
| `promptsChangeHandler(ListChangedHandler)` | none | |
| `resourceUpdatedHandler(ResourceUpdatedHandler)` | none | Receives `notifications/resources/updated` with the URI |

Capabilities are derived from what you register: `roots` with `listChanged` is always declared,
`sampling` and `elicitation` only when a handler for each is configured.

## Initialize

```java
Future<McpSchema.InitializeResult> ready = client.initialize();
```

`initialize()` connects the transport, sends `initialize`, then sends
`notifications/initialized`. Repeat calls return the same future, so it is safe to treat as an
idempotent "make sure we are ready".

The client offers the newest version its transport supports. If the server picks something the
transport does not speak, the future fails with `IllegalStateException` rather than guessing — the
specification is explicit that a client which cannot speak the chosen version must disconnect.

Every other method fails with `IllegalStateException` until `initialize()` has completed. There is
no implicit lazy handshake, because a half-finished handshake behind a `tools/call` is a very
confusing thing to debug.

After initialize:

```java
client.initializeResult();      // McpSchema.InitializeResult, or null before initialize
client.serverInfo();            // McpSchema.Implementation
client.serverCapabilities();    // McpSchema.ServerCapabilities
client.protocolVersion();       // the negotiated version
client.isInitialized();         // boolean
client.clientInfo();
client.clientCapabilities();
client.session();               // the underlying McpClientSession, for anything not wrapped here
```

`client.close()` closes the session, which closes the transport. On Streamable HTTP with
`terminateSessionOnClose(true)` (the default) that also sends a `DELETE` so the server can drop the
session's state immediately.

## Tools

```java
Future<McpSchema.ListToolsResult> listTools();
Future<McpSchema.ListToolsResult> listTools(String cursor);
Future<List<McpSchema.Tool>>      listAllTools();

Future<McpSchema.CallToolResult>  callTool(McpSchema.CallToolRequest request);
```

Calling a tool, with a progress token so the server will report progress:

```java
client.callTool(McpSchema.CallToolRequest.builder()
    .name("forecast")
    .arguments(Map.of("city", "Paris"))
    .meta(Map.of("progressToken", "forecast-1"))
    .build())
  .onSuccess(result -> result.content().forEach(content -> {
    if (content instanceof McpSchema.TextContent text) {
      System.out.println(text.text());
    }
  }));
```

A tool that failed in band comes back as a normal success with `result.isError() == true`. A tool
whose handler broke fails the future with `McpError`, from which you can read the JSON-RPC code:

```java
client.callTool(new McpSchema.CallToolRequest("explode", Map.of()))
    .onFailure(cause -> {
      if (cause instanceof McpError error) {
        logger.warn("code {}: {}", error.getJsonRpcError().code(), error.getJsonRpcError().message());
      }
    });
```

## Resources

```java
Future<McpSchema.ListResourcesResult>         listResources();
Future<McpSchema.ListResourcesResult>         listResources(String cursor);
Future<List<McpSchema.Resource>>              listAllResources();

Future<McpSchema.ListResourceTemplatesResult> listResourceTemplates();
Future<McpSchema.ListResourceTemplatesResult> listResourceTemplates(String cursor);
Future<List<McpSchema.ResourceTemplate>>      listAllResourceTemplates();

Future<McpSchema.ReadResourceResult>          readResource(String uri);
Future<McpSchema.ReadResourceResult>          readResource(McpSchema.ReadResourceRequest request);

Future<Void>                                  subscribeResource(String uri);
Future<Void>                                  unsubscribeResource(String uri);
```

```java
client.readResource("weather://forecast/Berlin")
    .onSuccess(result -> result.contents().forEach(contents -> {
      if (contents instanceof McpSchema.TextResourceContents text) {
        System.out.println(text.text());
      }
    }));
```

`subscribeResource` / `unsubscribeResource` require the server to advertise the `subscribe` resource
capability. Check `client.serverCapabilities().resources().subscribe()` before calling; a server
that does not support it answers `METHOD_NOT_FOUND`. Note that mcp4vertx's *own* server does not
implement per-resource subscriptions — it broadcasts `resources/updated` to every session instead,
so your `resourceUpdatedHandler` fires without you having subscribed. See
[Building a server](server.md#resources).

## Prompts and completions

```java
Future<McpSchema.ListPromptsResult> listPrompts();
Future<McpSchema.ListPromptsResult> listPrompts(String cursor);
Future<List<McpSchema.Prompt>>      listAllPrompts();

Future<McpSchema.GetPromptResult>   getPrompt(McpSchema.GetPromptRequest request);
Future<McpSchema.CompleteResult>    completeCompletion(McpSchema.CompleteRequest request);
```

```java
client.getPrompt(new McpSchema.GetPromptRequest("travel-advice", Map.of("city", "Oslo")))
    .onSuccess(prompt -> prompt.messages().forEach(message -> System.out.println(message.content())));

client.completeCompletion(new McpSchema.CompleteRequest(
        new McpSchema.PromptReference("travel-advice"),
        new McpSchema.CompleteRequest.CompleteArgument("city", "Pa"),
        null))
    .onSuccess(result -> System.out.println(result.completion().values()));
```

## Logging and liveness

```java
client.setLoggingLevel(McpSchema.LoggingLevel.WARNING);   // filter notifications/message
client.ping();                                            // liveness probe on an idle connection
```

## Cursor-walking helpers

`listAllTools()`, `listAllResources()`, `listAllResourceTemplates()`, and `listAllPrompts()` walk
`nextCursor` to the end and return a flat list:

```java
client.listAllTools().onSuccess(tools -> tools.forEach(tool -> System.out.println(tool.name())));
```

The walk stops when `nextCursor` is null, empty, or equal to the cursor that was just sent — a server
that echoes the cursor back unchanged would otherwise loop forever.

These are conveniences over the paged calls, not a replacement. If a server's registry is large
enough that you care about paging, drive `listTools(cursor)` yourself.

## Handling server-initiated traffic

MCP is bidirectional. A server can call back into the client, and each of these is answered by a
handler you register on the builder.

### Sampling

The server asks the client's model to complete a message, so the server never needs model
credentials. Return a `Future` — answering may take a while.

```java
VertxMcpClient.builder(vertx)
    .sampling(request -> {
      String question = ((McpSchema.TextContent) request.messages().get(0).content()).text();
      return myModel.complete(question, request.systemPrompt(), request.maxTokens())
          .map(answer -> new McpSchema.CreateMessageResult(
              McpSchema.Role.ASSISTANT,
              new McpSchema.TextContent(answer),
              "my-model-v1",
              McpSchema.CreateMessageResult.StopReason.END_TURN));
    })
    .build();
```

With no `sampling` handler registered the client does not declare the capability, and a server that
asks anyway gets `METHOD_NOT_FOUND` with a message naming the missing handler.

### Elicitation

The server needs structured input from the user.

```java
.elicitation(request -> ui.askUser(request.message(), request.requestedSchema())
    .map(answers -> new McpSchema.ElicitResult(
        McpSchema.ElicitResult.Action.ACCEPT, answers)))
```

Decline with `Action.DECLINE` (or `Action.CANCEL`) rather than failing the future. A refusal is a
valid answer, not an error, and the server is entitled to handle it as such.

### Roots

Roots are answered from the client's own list — there is no handler to write.

```java
VertxMcpClient client = VertxMcpClient.builder(vertx)
    .root(new McpSchema.Root("file:///home/me/project", "project"))
    .build();

client.roots();                          // List<McpSchema.Root>, a copy
client.addRoot(new McpSchema.Root("file:///tmp", "scratch"));   // Future<Void>
client.removeRoot("file:///tmp");
client.rootsListChangedNotification();   // announce explicitly
```

`addRoot` replaces any existing root with the same URI, and both `addRoot` and `removeRoot` send
`notifications/roots/list_changed` — but only once `initialize` has completed, since before that the
server has nothing to invalidate.

### Notifications

Progress, logging, `list_changed`, and `resources/updated` are delivered to the handlers you
registered. They return `void`, because there is nothing to write back:

```java
VertxMcpClient.builder(vertx)
    .progressHandler(p -> logger.info("{}/{} {}", p.progress(), p.total(), p.message()))
    .loggingHandler(n -> logger.info("[{}] {}", n.logger(), n.data()))
    .toolsChangeHandler(() -> toolCache.invalidate())
    .resourcesChangeHandler(() -> resourceCache.invalidate())
    .promptsChangeHandler(() -> promptCache.invalidate())
    .resourceUpdatedHandler(uri -> refetch(uri))
    .build();
```

`list_changed` notifications carry no payload by design: re-list to see what changed.

Progress notifications for a `tools/call` arrive on the same POST response as the result, so by the
time the call's future completes, every progress notification the handler emitted has already
reached your handler. That ordering is a property of the transport, not a coincidence — see
[Threading model](threading.md).

Unsolicited notifications (`list_changed`, `resources/updated`) arrive on the standalone `GET`
stream, which the Streamable HTTP transport opens lazily after the first exchange. If you disable it
with `openListeningStream(false)`, you will not receive them.

## Driving the client from a non-Vert.x thread

Everything returns `Future`, so from a `main` method or a JUnit test running on a plain thread,
bridge out:

```java
McpSchema.InitializeResult init = client.initialize()
    .toCompletionStage()
    .toCompletableFuture()
    .get(20, TimeUnit.SECONDS);
```

Never block a Vert.x context this way. Inside a verticle, compose futures instead.
