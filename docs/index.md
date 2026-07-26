---
title: Overview
nav_order: 1
---

# mcp4vertx

A Vert.x-native SDK for the [Model Context Protocol](https://modelcontextprotocol.io). It gives you
an MCP server and an MCP client whose entire API is `io.vertx.core.Future`, running on the Vert.x
event loop with no Reactor anywhere on the compile classpath.

## What it is

Three artifacts:

| Artifact | Contents |
|---|---|
| `mcp4vertx-core` | JSON-RPC codec, sessions, transport SPI, event store |
| `mcp4vertx-server` | `VertxMcpServer`, the feature registry, four server transports |
| `mcp4vertx-client` | `VertxMcpClient` and three client transports |

The wire types — `McpSchema.Tool`, `McpSchema.CallToolResult`, everything under
`io.modelcontextprotocol.spec.McpSchema` — are reused verbatim from the official SDK's `mcp-core`,
with `reactor-core` excluded. Protocol revisions churn the DTOs; that stays upstream's problem. The
concurrency model is this project's.

## When to use it

Use mcp4vertx when your application is already a Vert.x application — a verticle, a vertx-web
service, a Quarkus reactive app — and you want the MCP endpoint to be another route on the same
router, sharing the same event loop, the same tracing context, and the same backpressure.

The specific thing you get is **context affinity**: a tool handler always runs with a live
`Vertx.currentContext()`, so context-local storage, tracing propagation, and ordered execution keep
working across the whole request. That is not achievable by layering Vert.x transports under a
Reactor-based protocol core. See [Threading model](threading.md) for why.

Use the official `io.modelcontextprotocol.sdk` instead when you are on Spring WebFlux, on a servlet
container, or anywhere else Reactor is already the ambient concurrency model. It is a good library
aimed at a different runtime. See [Migrating](migrating.md) for the API mapping if you change your
mind either way.

## Install

```xml
<dependency>
  <groupId>io.github.mcp4vertx</groupId>
  <artifactId>mcp4vertx-server</artifactId>
  <version>0.1.0</version>
</dependency>

<dependency>
  <groupId>io.github.mcp4vertx</groupId>
  <artifactId>mcp4vertx-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

Requirements: Java 17, Vert.x 5.1.5, Jackson 2. Both artifacts depend on `mcp4vertx-core`, which
depends on `vertx-core`, `jackson-databind`, `mcp-core`, and `slf4j-api`. The server module adds
`vertx-web`.

`com.networknt:json-schema-validator` is declared `<optional>true</optional>`. Add it yourself if
you want structured tool output validated against its `outputSchema`:

```xml
<dependency>
  <groupId>com.networknt</groupId>
  <artifactId>json-schema-validator</artifactId>
  <version>1.5.9</version>
</dependency>
```

Note the deliberate Jackson choice: this SDK does not use upstream's `mcp-json-jackson3` module, so
you do not end up with two Jackson majors on the classpath alongside the Jackson 2 that Vert.x
already uses.

## Five-minute quickstart

### A server

```java
package example;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.server.transport.StreamableHttpServerTransport;

public final class Quickstart {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    McpSchema.Tool forecast = McpSchema.Tool.builder()
      .name("forecast")
      .description("Multi-day forecast for a city.")
      .inputSchema(new McpSchema.JsonSchema("object",
          Map.of("city", Map.of("type", "string")), List.of("city"), false, null, null))
      .build();

    VertxMcpServer server = VertxMcpServer.builder(vertx)
      .serverInfo("weather", "1.0.0")
      .instructions("Ask for a forecast by city.")
      .tool(forecast, (exchange, request) -> {
        String city = String.valueOf(request.arguments().get("city"));
        return exchange.progress(0.5, 1.0, "calling upstream")
          .map(v -> McpSchema.CallToolResult.builder()
            .addTextContent(city + ": 18C, light rain, wind 12 kph")
            .build());
      })
      .build();

    Router app = Router.router(vertx);
    app.get("/health").handler(rc -> rc.response().end("ok"));
    app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());

    vertx.createHttpServer().requestHandler(app).listen(8080);
  }
}
```

Two things are worth noticing.

`exchange.progress(...)` writes onto the very SSE response this `tools/call` arrived on, ahead of
the result. That is what the Streamable HTTP transport exists for.

The transport hands you a `Router`, not an `HttpServer`. Mounting it as a sub-router means your own
routes, auth handlers, and CORS policy sit in front of it. See [Transports](transports.md).

### A client

```java
package example;

import java.util.Map;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Vertx;
import io.github.mcp4vertx.client.VertxMcpClient;
import io.github.mcp4vertx.client.transport.StreamableHttpClientTransport;

public final class QuickstartClient {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(StreamableHttpClientTransport.builder(vertx)
        .baseUrl("http://localhost:8080")
        .endpoint("/mcp")
        .build())
      .clientInfo("quickstart", "1.0.0")
      .progressHandler(progress -> System.out.println(".. " + progress.message()))
      .build();

    client.initialize()
      .compose(init -> client.listAllTools())
      .compose(tools -> {
        tools.forEach(tool -> System.out.println("tool: " + tool.name()));
        return client.callTool(McpSchema.CallToolRequest.builder()
          .name("forecast")
          .arguments(Map.of("city", "Paris"))
          .meta(Map.of("progressToken", "forecast-1"))
          .build());
      })
      .onSuccess(result -> System.out.println(result.content()))
      .eventually(client::close);
  }
}
```

Progress is opt-in on the client side: without a `progressToken` in `_meta`, the server's
`exchange.progress(...)` is a no-op, so handlers can call it unconditionally.

## Where to go next

- [Building a server](server.md) — tools, resources, templates, prompts, completions, dynamic
  registry, pagination, structured output.
- [Transports](transports.md) — the four server transports and three client transports, exact
  endpoints and headers, and how to mount alongside existing routes.
- [Client](client.md) — the full `VertxMcpClient` surface, cursor walking, and answering
  server-initiated sampling, elicitation, and `roots/list`.
- [Threading model](threading.md) — context pinning, lock-free session state, blocking work.
- [Resumability and scaling](resumability.md) — `Last-Event-ID`, the `EventStore` SPI, and how to
  run more than one node.
- [Migrating from the official SDK](migrating.md).
- [Conformance](conformance.md) — how correctness is established.
