# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below 1.0.0, the public API is not yet stable: a minor version bump may
contain a breaking change. Anything that breaks is listed under **Changed** or **Removed**
with a note on how to migrate.

## [Unreleased]

Nothing yet.

## [0.1.0] - TBD

<!--
  TODO (maintainer): replace TBD with the release date, in ISO 8601, at the moment you cut
  the tag. See RELEASING.md.
-->

First public release. mcp4vertx is a Vert.x-native SDK for the
[Model Context Protocol](https://modelcontextprotocol.io): an MCP server and an MCP client
whose entire API returns `io.vertx.core.Future`, with no Reactor on the compile classpath.

### Added

**Modules.** Three published artifacts under the `io.github.mcp4vertx` group:
`mcp4vertx-core` (JSON-RPC codec, sessions, transport SPI, event store), `mcp4vertx-server`
and `mcp4vertx-client`. Java 17, Vert.x 5.1.5, Jackson 2 only.

**Server** — `VertxMcpServer`, with a builder covering:

- Tools, with both `Future`-returning and blocking (`executeBlocking`) handlers, and
  structured output validated against an optional `outputSchema`.
- Resources, addressed by exact URI or by RFC 6570 template.
- Prompts, with arguments and multi-message results.
- Completions, per prompt or per resource template.
- Server-to-client calls: progress notifications, logging with `logging/setLevel`
  filtering, sampling, elicitation and `roots/list`.
- A dynamic feature registry — `addTool` / `removeTool` and the equivalents for resources
  and prompts — which emits the corresponding `list_changed` notifications.
- Opt-in cursor-based pagination via `pageSize`.

**Server transports** — four:

| Transport | Class | Protocol revision |
|---|---|---|
| Streamable HTTP | `StreamableHttpServerTransport` | 2025-03-26 and later |
| HTTP+SSE (legacy) | `SseServerTransport` | 2024-11-05 |
| Stateless HTTP | `StatelessHttpServerTransport` | no sessions, no SSE |
| stdio | `StdioServerTransport` | local subprocess |

The HTTP transports expose a vertx-web `Router` rather than creating an `HttpServer`, so an
MCP endpoint mounts as a sub-router alongside an application's existing routes, auth
handlers and CORS policy.

**Client** — `VertxMcpClient`, and three transports: `StreamableHttpClientTransport`,
`SseClientTransport` and `StdioClientTransport`.

**Resumability.** `Last-Event-ID` replay on the streamable HTTP transport, working in both
directions: a client that loses its stream reconnects and receives the events it missed. The
replay sits behind a four-method `EventStore` SPI, with `InMemoryEventStore` as the default
so a single-node deployment needs no configuration, and a clustered deployment can supply
its own store.

**Threading.** A session is pinned to the Vert.x `Context` it was created on, and every
handler runs on that context. `Vertx.currentContext()` is live for the whole of a tool
handler, so context-local storage, tracing propagation and ordered execution keep working
across a request. This is the property the project exists to provide; see
[`docs/threading.md`](docs/threading.md).

**Conformance.** The test suite exercises interoperability with the official
`io.modelcontextprotocol.sdk` in both directions:

- the official `McpClient.sync`, over both `HttpClientStreamableHttpTransport` and
  `HttpClientSseClientTransport`, driving a mcp4vertx server;
- `VertxMcpClient` driving the official `HttpServletStreamableServerTransportProvider`
  hosted in embedded Jetty 12.

81 tests across the four modules, run on JDK 17 and JDK 21 in CI. See
[`docs/conformance.md`](docs/conformance.md).

### Notes on this release

- The wire types are not this project's own: everything under
  `io.modelcontextprotocol.spec.McpSchema` is reused verbatim from the official SDK's
  `mcp-core` artifact, with `reactor-core` excluded. Schema-level code therefore ports
  between the two SDKs unchanged. See [`docs/migrating.md`](docs/migrating.md).
- `com.networknt:json-schema-validator` is an *optional* dependency. When it is on the
  classpath, structured tool output is validated against its `outputSchema`; when it is
  absent, that validation is skipped rather than failing.
- `mcp4vertx-examples` is a demo and conformance harness. It is built and tested on every
  run but is deliberately not published to Maven Central.
- This is a 0.x release and the public API may still change. The threading contract
  described above is the part least likely to.

[Unreleased]: https://github.com/mcp4vertx/mcp4vertx/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mcp4vertx/mcp4vertx/releases/tag/v0.1.0
