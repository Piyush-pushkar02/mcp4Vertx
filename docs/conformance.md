---
title: Conformance
nav_order: 8
---

# Conformance

An MCP implementation that only tests against itself proves self-consistency, not conformance. Two
implementations can agree perfectly on a shared misreading of the specification.

So the suite here runs **both directions against the official SDK**: the official client drives this
server, and this client drives the official server. Neither side of that pairing can be wrong about
the wire format without a test failing.

```
mvn verify
```

**81 tests** across four modules.

| Module | Tests |
|---|---|
| `mcp4vertx-core` | 3 |
| `mcp4vertx-server` | 4 |
| `mcp4vertx-client` | 43 |
| `mcp4vertx-examples` | 31 |

## Cross-SDK conformance

### The official client against this server

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/OfficialSdkConformanceTest.java`
— 5 tests.

The official `io.modelcontextprotocol.sdk` synchronous client — a different codebase, a different
HTTP stack, a different JSON mapper — drives a `VertxMcpServer` over both HTTP transports.

Over `HttpClientStreamableHttpTransport`:

- The handshake: server info, version, instructions, and the tools, resources, and prompts
  capabilities. Upstream offers `2025-11-25` as its newest and this server speaks it, so the
  assertion is that `2025-11-25` wins the negotiation.
- `tools/list` and `tools/call`, in three shapes: a success, an in-band failure (a normal result
  with `isError` set), and a handler that throws (a JSON-RPC error that upstream raises as
  `McpError`).
- `resources/list`, `resources/templates/list`, and `resources/read` for both an exact URI and a
  templated one.
- `prompts/list` and `prompts/get` with an argument.

Over `HttpClientSseClientTransport` (the legacy 2024-11-05 pair):

- The handshake, asserting the negotiated version is `2024-11-05`, then `tools/list` and
  `tools/call`.

The upstream client is Reactor-based and blocks on every call, so every test runs on the plain JUnit
thread and never on a Vert.x context. The Vert.x server is booted once for the class and awaited
with an explicit timeout.

### This client against the official server

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/ReverseConformanceTest.java`
— 3 tests.

The official SDK's `HttpServletStreamableServerTransportProvider`, hosted in **Jetty 12** with the
`ee10` servlet stack, serves an `McpSyncServer`. `VertxMcpClient` is the implementation under test.

- The handshake against an implementation this repository did not write: server info, version,
  instructions, capabilities, negotiated protocol version.
- `listAllTools()` and `callTool`.
- `listAllResources()`, `readResource` for an exact URI and a templated one, and `getPrompt` with an
  argument.

The upstream server is blocking and servlet-based, so it is started and stopped on the plain JUnit
thread; the client's futures are awaited with an explicit timeout rather than by blocking a Vert.x
context.

{: .note }
One deliberate wrinkle in both classes: upstream's default `JsonSchemaValidator` needs
`com.networknt:json-schema-validator` 3.x, which shares its package with the 1.x this repository
compiles against. Only one can be on a classpath, so the upstream client and server are handed a
permissive validator. What is under test is protocol behavior, and this server does its own
output-schema validation regardless — asserted separately below.

## End-to-end protocol surface

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/StreamableHttpEndToEndTest.java`
— 16 tests. A `VertxMcpClient` against a `VertxMcpServer` over Streamable HTTP and a real socket.

| Area | What is asserted |
|---|---|
| Handshake | Negotiated version, server info, instructions, and the tools/resources/prompts/logging capabilities |
| Tools | `tools/list` returns every registered tool; `tools/call` echoes its argument |
| Tools | A handler that fails surfaces as a JSON-RPC `INTERNAL_ERROR` carrying the cause's message |
| Structured output | A tool with an `outputSchema` returns `structuredContent` the client can read |
| Structured output | `structuredContent` that **violates** the declared schema fails with `INTERNAL_ERROR` and "violates its outputSchema" |
| Progress | Three progress notifications emitted mid-handler arrive on the same POST stream, in order, ahead of the result |
| Progress | No progress is sent when the client supplied no `progressToken` |
| Sampling | A tool issues `sampling/createMessage` from inside its handler; the answer is correlated back to that handler and used in the result |
| Sampling | A client that declares no sampling capability gets a clean `does not support sampling` error rather than a hang |
| Resources | List, read by exact URI, and read through an RFC 6570 template |
| Resources | An unknown URI fails with MCP's `RESOURCE_NOT_FOUND` code |
| Prompts | List with arguments, and render |
| Logging | `logging/setLevel(ERROR)` filters out a subsequent `INFO` line while the `ERROR` line still arrives |
| List-changed | `server.addTool(...)` at runtime reaches a connected client's `toolsChangeHandler` over the standalone GET stream |
| Session teardown | `DELETE` returns 200, and a later request fails with `McpSessionNotFoundException` |
| Concurrency | 20 concurrent `tools/call` on one session each get their own result, with no cross-delivery |

Nothing in this class sleeps. Ordering assertions rely on the fact that a single SSE response is
delivered in order on one Vert.x context, so by the time a call's future completes, everything the
handler wrote before the result has already reached the client's handlers. The one place a
synchronization point does not exist on the wire — the lazily opened standalone GET stream — is
handled by re-announcing until the client acknowledges, not by a timer.

## Resumability

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/ResumabilityTest.java` — 2 tests,
driven with a raw `HttpClient` so the test controls precisely when the socket dies and what the
reconnect asks for.

- A severed listening stream resumes from `Last-Event-ID` and receives exactly the messages recorded
  after that event: every one of them, each exactly once, in order, and nothing it had already seen.
  The anchor event itself is not re-sent, and nothing appears that the live stream never recorded.
- An unknown `Last-Event-ID` still yields a working stream. Resumption is best-effort: a stale id
  costs the client its replay, not its stream.

`mcp4vertx-core/src/test/java/io/github/mcp4vertx/session/McpStreamableServerSessionTest.java`
— 3 tests, covering the two properties that make replay trustworthy:

- Notifications emitted while **no listening stream was bound** are recorded and replayed on
  reconnect. This is the case a naive implementation drops silently.
- A send with no listening stream at all succeeds rather than failing, and the message is recorded.
- Recorded events are purged when the session ends, so a long-lived server does not leak
  proportionally to total requests served.

## Server transport

`mcp4vertx-server/src/test/java/io/github/mcp4vertx/server/transport/StreamableHttpServerTransportTest.java`
— 4 tests, at the HTTP level.

- `initialize` returns the session id in `Mcp-Session-Id` and the result as plain JSON.
- A POST whose `Accept` does not cover both `application/json` and `text/event-stream` is rejected
  with 406.
- An unknown session id is rejected with 404 and the session-not-found JSON-RPC code, distinct from
  the 400 a missing header gets.
- The listening stream receives broadcasts, and `DELETE` terminates the session.

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/LegacyTransportsTest.java` — 2 tests.

- The 2024-11-05 HTTP+SSE pair completes a handshake over its split `GET /sse` + `POST /message`
  channels and calls a tool.
- The stateless transport answers a bare POST with a plain JSON-RPC result, and refuses `GET` with
  405 and a message explaining that only POST is supported.

## Client

`mcp4vertx-client/src/test/java/io/github/mcp4vertx/client/VertxMcpClientTest.java` — 8 tests.

- Requests before `initialize()` are refused.
- The handshake sends `initialize` then `notifications/initialized`.
- A protocol version the client cannot speak is refused rather than guessed at.
- `listAllTools()` walks cursors to the end.
- `roots/list` is answered from the configured roots.
- Sampling with no handler is reported as unsupported; with a handler, it is delegated.
- Server notifications are routed to their registered handlers.

`StreamableHttpClientTransportTest` — 5 tests: every SSE message is delivered as it arrives and the
session id is captured from the response head; a single JSON response is decoded; a `202` is treated
as nothing to deliver; an expired session is reported distinctly as
`McpSessionNotFoundException`; the standalone stream is opened once the session exists.

`ListeningStreamReconnectTest` — 2 tests: the stream is reopened with `Last-Event-ID` after the
server drops it, and a server that declines the stream (405/501) is accepted without retrying.

`SseClientTransportTest` — 3 tests: `connect()` waits for the `endpoint` event and then posts there;
messages arriving on the stream are delivered; a stream that dies before the endpoint event fails
`connect()`.

`StdioClientTransportTest` — 3 tests: a message round-trips through a real child process; stderr is
piped to the configured consumer; a failure to start the child is reported.

### SSE parser

`SseParserTest` — 22 tests, and the most paranoid file in the suite. The parser is fed by a socket,
so the only thing that really matters is that it behaves identically no matter where the chunk
boundaries fall. Most cases assert the same expected events for a whole-stream write, a
byte-at-a-time write, and **every single split position in between**.

Covered: single events, explicit event types and their reset after each event, multiple `data:`
fields joined with newlines, the one-leading-space stripping rule, a field with no colon, comments,
unknown fields, a blank line with no data, `lastEventId` carried forward across events that omit it,
an id containing NUL, `retry:` including a non-numeric one, CRLF and lone-CR line endings, a CRLF
straddling a chunk boundary, multi-byte characters split across chunks, an event never terminated by
a blank line, a trailing line with no terminator at all, idempotent `end()`, and a refusal to buffer
an unbounded line.

## The examples are tested too

`mcp4vertx-examples/src/test/java/io/github/mcp4vertx/examples/ExamplesSmokeTest.java` — 3 tests.
The two files in `src/main` are working documentation rather than pseudocode:

- `WeatherServerVerticle` deploys and serves its non-MCP `/health` route and its MCP sub-router off
  the same port.
- Every feature it advertises answers correctly: the async tool with progress, the blocking tool,
  the static resource, the templated resource, and the prompt.
- `ClientExample` drives it to completion and prints what it claims to print.

Any snippet lifted from those two files into these docs is therefore covered by a test.
