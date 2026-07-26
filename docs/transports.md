---
title: Transports
nav_order: 3
---

# Transports

A `VertxMcpServer` is transport-agnostic. Attach one transport, or several at once — each registers
itself as a broadcaster on the server when built, so `list_changed` notifications reach clients on
all of them.

## Choosing one

| You are | Use |
|---|---|
| Serving remote MCP clients from a vertx-web app | `StreamableHttpServerTransport` |
| Serving remote clients that predate 2025-03-26 | `SseServerTransport`, usually alongside the streamable one |
| Deploying to Lambda, Cloud Run, or a fleet with no sticky routing | `StatelessHttpServerTransport` |
| Shipping a local server a desktop MCP host launches as a subprocess | `StdioServerTransport` |

On the client side: `StreamableHttpClientTransport` for anything modern,
`SseClientTransport` for a 2024-11-05 server, `StdioClientTransport` to run a server as a child
process.

## Server transports

### Streamable HTTP

One endpoint, three methods. This is the current transport (protocol 2025-03-26 and later).

```java
StreamableHttpServerTransport transport = StreamableHttpServerTransport.create(vertx, server);
router.route("/mcp*").subRouter(transport.router());
```

Or with options:

```java
StreamableHttpServerTransport transport = StreamableHttpServerTransport.builder(vertx, server)
    .mcpEndpoint("/mcp")
    .allowedOrigins("https://app.example.com")
    .keepAliveInterval(Duration.ofSeconds(20))
    .disallowDelete(false)
    .contextExtractor(rc -> McpTransportContext.of(
        Map.of("tenant", String.valueOf(rc.request().getHeader("X-Tenant-Id")))))
    .build();
```

| Option | Default | Meaning |
|---|---|---|
| `mcpEndpoint(String)` | `/mcp` | Path of the single endpoint |
| `allowedOrigins(String...)` | empty (check disabled) | `Origin` allow-list for DNS-rebinding protection |
| `disallowDelete(boolean)` | `false` | Reject `DELETE` with 405, for deployments where sessions expire on their own |
| `keepAliveInterval(Duration)` | `null` (off) | SSE comment interval on an idle listening stream |
| `contextExtractor(Function<RoutingContext, McpTransportContext>)` | headers + `RoutingContext` | What transport metadata reaches handlers |
| `codec(JsonRpcCodec)` | the server's | JSON-RPC serialization |

#### Wire behavior

**`POST /mcp`** carries every client-to-server message. `Accept` must include *both*
`application/json` and `text/event-stream` (or `*/*`) — the client cannot know in advance which it
will get. `406` otherwise.

| Body | Response |
|---|---|
| `initialize` request, no `Mcp-Session-Id` | `200 application/json` with the result, and the new id in the `Mcp-Session-Id` response header |
| Notification | `202 Accepted`, empty body |
| Response to a server-initiated request | `202 Accepted`, empty body |
| Any other request | `200 text/event-stream` — an SSE stream carrying anything the handler emits, then the result, then close |

The SSE response for a request is the whole point of the transport: progress notifications, log
lines, and reverse requests (`sampling/createMessage`, `elicitation/create`) that a handler issues
while it runs ride back on the same HTTP response, ahead of the result.

**`GET /mcp`** opens the standalone listening stream for unsolicited server-to-client traffic.
`Accept` must include `text/event-stream`. Send `Last-Event-ID` to resume; see
[Resumability](resumability.md). The transport flushes a `: connected` comment immediately so the
client observes the `200` without waiting for traffic.

**`DELETE /mcp`** terminates the session and answers `200`. With `disallowDelete(true)` it answers
`405`.

#### Headers

| Header | Direction | Notes |
|---|---|---|
| `Mcp-Session-Id` | response on `initialize`, request thereafter | Required on every non-`initialize` request |
| `MCP-Protocol-Version` | request | Validated against `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`. Absent is allowed (the client predates the header); anything else is a `400` |
| `Last-Event-ID` | request on `GET` | Resume point |
| `Origin` | request | Checked against `allowedOrigins` when that set is non-empty |

#### Status codes

| Code | When |
|---|---|
| `400` | Unparseable body, missing `Mcp-Session-Id`, unsupported `MCP-Protocol-Version` |
| `403` | `Origin` not in the allow-list |
| `404` | Session not found — JSON-RPC error code `-32001`. Distinct from `400`: the server forgot a session the client still believes in, and the client should re-initialize |
| `405` | `DELETE` with `disallowDelete(true)` |
| `406` | `Accept` does not cover what the response needs |
| `503` | Transport is shutting down |

The SSE responses carry `Cache-Control: no-cache`, `Connection: keep-alive`, and
`X-Accel-Buffering: no` — the last is the conventional opt-out for nginx and friends, which
otherwise buffer unknown content types and stall the stream until the buffer fills.

#### Origin checking

`allowedOrigins` is empty by default, which disables the check. Set it for any server bound to
localhost: a browser page on another origin can reach `127.0.0.1` but cannot forge `Origin`, so the
check is what stops DNS rebinding. An absent `Origin` header passes — that means a non-browser
client, which this check is not aimed at.

### Legacy HTTP+SSE

Protocol 2024-11-05, kept for clients that have not moved. Two endpoints instead of one.

```java
SseServerTransport transport = SseServerTransport.create(vertx, server);
router.route("/*").subRouter(transport.router());
```

| Option | Default |
|---|---|
| `sseEndpoint(String)` | `/sse` |
| `messageEndpoint(String)` | `/message` |
| `baseUrl(String)` | `""` — prefix prepended to the advertised message endpoint, for when a reverse proxy mounts the server under a path it never sees |
| `allowedOrigins(String...)` | empty |
| `keepAliveInterval(Duration)` | `null` |
| `contextExtractor(...)` | headers + `RoutingContext` |

**`GET /sse`** opens a long-lived stream. Its very first frame is an `endpoint` event whose data is
the message URL with the session id appended as a query parameter:

```
event: endpoint
data: /message?sessionId=6f2b1e4c-...
```

That handshake is what the 2024-11-05 spec mandates, which is why the session id is a query
parameter here and not an HTTP header. The parameter name is
`SseServerTransport.SESSION_ID_PARAM` (`"sessionId"`).

**`POST /message?sessionId=...`** carries everything the client says. The reply, if any, goes out on
the SSE stream, not on this response, so the POST is answered `202 Accepted` as soon as the message
is handed to the session.

Because the two paths are distinct, mount this router at a prefix that contains both — typically the
application root, or give it its own port.

### Stateless HTTP

Every POST is answered inline and nothing at all is retained between calls.

```java
router.route("/mcp*").subRouter(StatelessHttpServerTransport.create(vertx, server).router());
```

| Option | Default |
|---|---|
| `mcpEndpoint(String)` | `/mcp` |
| `allowedOrigins(String...)` | empty |
| `contextExtractor(...)` | headers + `RoutingContext` |

`POST` returns `200 application/json` for a request and `202` for a notification. `GET` and
`DELETE` return `405`.

The trade-off is explicit: no sessions means no SSE, no reverse requests, and no notifications —
`exchange.createMessage(...)` fails with `UnsupportedOperationException` rather than quietly doing
nothing. In exchange it removes the only reason Streamable HTTP needs sticky routing, which makes it
the right shape for Lambda, Cloud Run, or any horizontally scaled fleet where consecutive requests
from one client land on different instances.

### stdio

Newline-delimited JSON-RPC on `System.in` and `System.out` — how a desktop MCP host launches and
talks to a local server process.

```java
StdioServerTransport.create(vertx, server).start();
```

`start()` returns a `Future<Void>` that completes when the input stream reaches EOF (the host closed
the pipe) or fails if reading broke. `create(vertx, server, in, out)` binds to arbitrary streams for
tests and embedding. The single session's id is `StdioServerTransport.SESSION_ID` (`"stdio"`).

**stdout is the protocol channel.** A single stray `println` corrupts the stream and the host drops
the connection. This class never routes anything through SLF4J — whose default console appender
writes to stdout — and logs to `System.err` directly instead. Configure your own logging the same
way when you deploy over stdio.

`System.in` has no non-blocking API, so the read loop runs on a Vert.x worker thread
(`ordered = false`, so it cannot starve the outbound writes) and hands each parsed message back to
the session's event-loop context. It parks that one worker thread for the life of the process,
which is acceptable precisely because stdio serves exactly one client and lives exactly as long as
the process does.

## Mounting alongside your own routes

Every HTTP transport hands you a `Router`, never an `HttpServer`. Mount it as a sub-router and your
own middleware sits in front of it:

```java
Router app = Router.router(vertx);

app.get("/health").handler(rc -> rc.json(new JsonObject().put("status", "UP")));

app.route("/mcp*").handler(CorsHandler.create().addOrigin("https://app.example.com"));
app.route("/mcp*").handler(JWTAuthHandler.create(jwtAuth));
app.route("/mcp*").subRouter(StreamableHttpServerTransport.create(vertx, server).router());

vertx.createHttpServer().requestHandler(app).listen(8080);
```

Authorization is deliberately out of scope for this SDK: mount your own vertx-web auth handler in
front of the sub-router, and read the authenticated principal back out through
`exchange.transportContext()`.

Ordering matters. vertx-web runs handlers in registration order, so register CORS and auth on the
same path pattern *before* the `subRouter` call.

The transport installs its own `BodyHandler.create(false)` (no file uploads) inside its sub-router,
so you do not need one, and one you install upstream will not conflict.

{: .note }
The router each transport returns registers its endpoint both at the configured path and at `"/"`.
vertx-web strips the mount prefix before a sub-router sees the request, so the endpoint arrives as
`"/"` when mounted, and as `/mcp` when the router is installed directly as an `HttpServer` request
handler. Both shapes work.

Call `router()` once per transport and mount that. Calling it twice builds two independent routers
over the same session map, which is rarely what you want.

### Narrowing the transport context

The default extractor puts every request header plus the `RoutingContext` into
`McpTransportContext`. Replace it to hand handlers exactly what they need:

```java
StreamableHttpServerTransport.builder(vertx, server)
    .contextExtractor(rc -> {
      Map<String, Object> values = new HashMap<>();
      if (rc.user() != null) {
        values.put("principal", rc.user().subject());
      }
      String tenant = rc.request().getHeader("X-Tenant-Id");
      if (tenant != null) {
        values.put("tenant", tenant);
      }
      return McpTransportContext.of(values);
    })
    .build();
```

`McpTransportContext.of(...)` copies the map, so a mutable one is fine, and an empty one returns the
shared `McpTransportContext.EMPTY`.

## Client transports

### Streamable HTTP

```java
var transport = StreamableHttpClientTransport.builder(vertx)
    .baseUrl("https://example.com")
    .endpoint("/mcp")
    .requestCustomizer(request -> request.putHeader("Authorization", "Bearer " + token))
    .build();
```

| Option | Default | Meaning |
|---|---|---|
| `baseUrl(String)` | `http://localhost:8080` | Scheme, host, port, and any path prefix |
| `endpoint(String)` | `/mcp` | Appended to the base URL |
| `httpClient(HttpClient)` | created internally | Supply your own. The transport will not close a client it did not create |
| `httpClientOptions(HttpClientOptions)` | defaults | Used only when the transport creates its own client |
| `header(String, String)` / `headers(Map)` | none | Sent on every request |
| `requestCustomizer(Function<HttpClientRequest, HttpClientRequest>)` | none | Last hook before send, for per-request credentials |
| `requestTimeout(Duration)` | 30s | **Idle** deadline on a POST, not a total one |
| `openListeningStream(boolean)` | `true` | Whether to open the standalone `GET` stream |
| `maxReconnectAttempts(int)` | `3` | Retries on the standalone stream before giving up |
| `reconnectDelay(Duration)` | 1s | First backoff step; doubles per attempt |
| `maxReconnectDelay(Duration)` | 30s | Backoff ceiling |
| `terminateSessionOnClose(boolean)` | `true` | Whether `close()` sends `DELETE` |
| `protocolVersions(List<String>)` | `[2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25]` | Offered at `initialize`, newest last. Matches the official SDK's default set. |

The transport is built directly on vertx-core's `HttpClient`, not on `vertx-web-client`, because the
JSON-versus-SSE decision has to be made from the response *head*: `HttpClientRequest#send` hands back
the `HttpClientResponse` as soon as the status line and headers arrive, which is the only moment at
which `Mcp-Session-Id` is early enough to be useful and `Content-Type` is known while the body is
still streaming.

`requestCustomizer` therefore receives a vertx-core `HttpClientRequest`, not the Web Client's
`HttpRequest`. The Web Client conveniences have plain equivalents:
`bearerTokenAuthentication(t)` becomes `putHeader("Authorization", "Bearer " + t)`.

The standalone `GET` stream is opened lazily, after the first exchange, because it has to carry
`Mcp-Session-Id` and that only exists once `initialize` has been answered. A server that declines it
with `405` or `501` is respected without retrying — the spec allows that, and there is simply
nothing to listen to. `requestTimeout` deliberately does not apply to it: a server is entitled to
keep that stream open and silent for as long as it likes.

A `404` on any request surfaces as `McpSessionNotFoundException`, distinct from a generic
`McpTransportException`, and the stale session id is dropped so a re-initialize starts clean.

### Legacy HTTP+SSE

```java
var transport = SseClientTransport.builder(vertx)
    .baseUrl("http://localhost:8080")
    .sseEndpoint("/sse")
    .build();
```

Options: `baseUrl`, `sseEndpoint` (default `/sse`), `httpClient`, `httpClientOptions`, `header` /
`headers`, `requestCustomizer`, `requestTimeout`, `codec`.

`connect()` does not complete until the server's `endpoint` event lands, because sending anything
before it would have nowhere to go. The advertised endpoint is resolved against the SSE URL, so both
a relative path with a query parameter and a fully qualified URL work.

`requestTimeout` is the deadline for a message POST. The SSE stream itself is never timed out.

The stream ending is always a failure for this transport — it is the only way the server can reach
the client. Before the endpoint event, `connect()` fails; after it, the failure goes to the
transport's exception handler.

### stdio

```java
var transport = StdioClientTransport.builder(vertx)
    .command("npx")
    .args("-y", "@modelcontextprotocol/server-filesystem", "/tmp")
    .env("LOG_LEVEL", "debug")
    .stderrConsumer(line -> logger.info("[server] {}", line))
    .build();
```

| Option | Default |
|---|---|
| `command(String)` / `command(List<String>)` | required |
| `args(String...)` / `args(List<String>)` | none |
| `env(String, String)` / `env(Map)` | none |
| `inheritEnvironment(boolean)` | `true` — set false to hand the child exactly the entries you gave it |
| `stderrConsumer(Consumer<String>)` | logs at INFO |
| `terminationGrace(Duration)` | 5s before `destroyForcibly()` |
| `protocolVersions(List<String>)` | `[2024-11-05, 2025-03-26, 2025-06-18]` |

`Process` only offers blocking streams, so the two readers and the writer live on Vert.x worker
threads and hand every decoded message back to the transport's own context. The reader pool is
`ordered = false` because stdout and stderr must be drained concurrently — a child that fills its
stderr pipe while the client is parked on stdout would deadlock. The writer pool is ordered, of size
one, so messages reach stdin in the order they were sent and no lock is needed.

`close()` closes the child's stdin (the polite shutdown signal for an MCP stdio server), then
`destroy()`, then `destroyForcibly()` after the grace period.
