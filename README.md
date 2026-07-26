# mcp4vertx

A Vert.x-native SDK for the [Model Context Protocol](https://modelcontextprotocol.io). Everything
returns `io.vertx.core.Future`. No Reactor on the compile classpath, no scheduler indirection, no
thread hop you did not ask for.

[![Maven Central](https://img.shields.io/maven-central/v/io.github.mcp4vertx/mcp4vertx-server?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.mcp4vertx/mcp4vertx-server)
[![Build](https://img.shields.io/github/actions/workflow/status/mcp4vertx/mcp4vertx/build.yml?branch=main&label=build)](https://github.com/mcp4vertx/mcp4vertx/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/projects/jdk/17/)

## A working MCP server

```java
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
    .tool(forecast, (exchange, request) -> exchange.progress(0.5, 1.0, "calling upstream")
        .map(v -> McpSchema.CallToolResult.builder()
            .addTextContent(request.arguments().get("city") + ": 18C, light rain")
            .build()))
    .build();

Router app = Router.router(vertx);
app.get("/health").handler(rc -> rc.response().end("ok"));
app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());
vertx.createHttpServer().requestHandler(app).listen(8080);
```

The MCP endpoint is a vertx-web sub-router. It composes with the auth handlers, CORS policy, and
body limits you already have, rather than owning the port.

## Why not just use the official SDK?

The official `io.modelcontextprotocol.sdk` is a good library. It targets a different runtime.

Its protocol core is built on Reactor, and Reactor has no notion of a Vert.x `Context`. Any hop to
`boundedElastic`, or any `Mono` completed by a non-Vert.x thread, leaves the continuation on a
thread where `Vertx.currentContext()` returns `null`. That silently breaks context-local storage,
tracing propagation, and ordered task execution. A custom `Scheduler` plus a `Mono`/`Future` bridge
papers over the seam, but it leaks: one `Mono.fromCallable(..).subscribeOn(boundedElastic())` in a
user's tool handler walks straight past the bridge.

So this SDK reuses exactly one thing from upstream — `McpSchema`, the wire DTOs, which import zero
Reactor classes — and implements sessions, lifecycle, routing, and every transport natively on
`Future`. `reactor-core` is excluded outright in the POM.

The practical consequence is that `McpSchema` types are *identical* between the two SDKs, so
schema-level code ports unchanged. See [Migrating](docs/migrating.md).

## Features

| | |
|---|---|
| Tools | Async (`Future`) and blocking (`executeBlocking`) handlers, structured output with optional `outputSchema` validation |
| Resources | Exact URIs and RFC 6570 templates |
| Prompts | Arguments, multi-message results |
| Completions | Per prompt or per resource template |
| Server to client | Progress, logging with `logging/setLevel` filtering, sampling, elicitation, `roots/list` |
| Dynamic registry | `addTool` / `removeTool` and friends, with `list_changed` broadcasts |
| Pagination | Opt-in, cursor-based (`pageSize`) |
| Resumability | `Last-Event-ID` replay behind a 4-method `EventStore` SPI, in-memory by default |

### Transports

| Transport | Server class | Client class | Protocol |
|---|---|---|---|
| Streamable HTTP | `StreamableHttpServerTransport` | `StreamableHttpClientTransport` | 2025-03-26 and later |
| HTTP+SSE (legacy) | `SseServerTransport` | `SseClientTransport` | 2024-11-05 |
| Stateless HTTP | `StatelessHttpServerTransport` | — | no sessions, no SSE |
| stdio | `StdioServerTransport` | `StdioClientTransport` | local subprocess |

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

Both pull in `mcp4vertx-core`. Java 17, Vert.x 5.1.5, Jackson 2 only.
`com.networknt:json-schema-validator` is an optional dependency: present, structured tool output is
validated against its `outputSchema`; absent, validation is skipped.

## Documentation

- [Overview and quickstart](docs/index.md)
- [Building a server](docs/server.md)
- [Transports](docs/transports.md)
- [Client](docs/client.md)
- [Threading model](docs/threading.md) — the reason this library exists
- [Resumability and scaling](docs/resumability.md)
- [Migrating from the official SDK](docs/migrating.md)
- [Conformance](docs/conformance.md)

## Conformance

The test suite runs both directions against the official SDK: the official `McpClient.sync` over
`HttpClientStreamableHttpTransport` and `HttpClientSseClientTransport` against this server, and
`VertxMcpClient` against the official `HttpServletStreamableServerTransportProvider` hosted in
Jetty 12. Details in [conformance.md](docs/conformance.md).

```
mvn verify
```

## License

Apache License 2.0.
