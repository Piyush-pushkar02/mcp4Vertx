---
title: Threading model
nav_order: 5
---

# Threading model

This is the page that explains why the library exists. Everything else — the transports, the
builders, the `Future`-shaped API — follows from one property: **a session is pinned to the Vert.x
context it was created on, and every handler runs on that context.**

## Context pinning

`AbstractMcpSession` captures a context in its constructor and never lets go of it:

```java
protected AbstractMcpSession(Vertx vertx, JsonRpcCodec codec, Duration requestTimeout, ...) {
  this.vertx = vertx;
  this.context = Contexts.current(vertx);
  ...
}
```

`Contexts.current(vertx)` is the caller's context if there is one, and a fresh one otherwise:

```java
public static Context current(Vertx vertx) {
  Context context = Vertx.currentContext();
  return context != null ? context : vertx.getOrCreateContext();
}
```

Every entry point into the session then hops onto that context before touching anything:

```java
public static <T> Future<T> on(Context context, Supplier<Future<T>> action) {
  if (isOn(context)) {
    try {
      return action.get();
    }
    catch (Throwable t) {
      return Future.failedFuture(t);
    }
  }
  Promise<T> promise = Promise.promise();
  context.runOnContext(v -> {
    try {
      action.get().onComplete(promise);
    }
    catch (Throwable t) {
      promise.tryFail(t);
    }
  });
  return promise.future();
}
```

Note the fast path. A call already on the right context runs inline, with no `runOnContext` and no
extra event-loop tick. Since most calls originate on the session's own context, the hop is the
exception rather than the rule.

`sendRequest`, `sendNotification`, `close`, `handle`, `responseStream`, `listeningStream`,
`resumeStream`, and `handleClientResponse` all go through `Contexts.on(...)`. So do the transports:
`SseWriter` pins to a context and serializes its write chain on it, and the client transports pin
too and deliver every inbound message via `Contexts.run(this.context, ...)`.

## Why session state needs no locks

Because all mutation happens on one context, and a Vert.x context executes its tasks one at a time,
the mutable state of a session is effectively single-threaded. `AbstractMcpSession` therefore holds:

```java
private final Map<Object, Pending> pendingRequests = new HashMap<>();
private final Set<Object> cancelledRequests = new HashSet<>();
```

Plain `HashMap` and `HashSet`. Not `ConcurrentHashMap`, not `synchronized`, not a `StampedLock`.
`McpStreamableServerSession` does the same for its stream registry:

```java
private SessionStream listeningStream;
private final Map<Object, SessionStream> requestIdToStream = new HashMap<>();
private final Set<SessionStream> openStreams = new HashSet<>();
```

Three things follow from that, and all three are things you would otherwise have to pay for:

**No lock contention.** Twenty concurrent `tools/call` requests on one session — a case the test
suite pins down explicitly — never contend on a monitor, because they are serialized by the context
rather than by a lock.

**Deterministic ordering.** Messages within a session leave in the order they were produced. A
handler that emits three progress notifications and then a result produces exactly that sequence on
the wire, every time, with no barrier or sequence number needed.

**No visibility bugs.** There is no publication problem between a write and a later read, because
there is no second thread to publish to.

The concurrency this buys back is *across* sessions, not within one. Sessions created on different
contexts run on different event loops in genuine parallel; a session is a serialization boundary,
which is exactly the granularity the protocol has anyway, since one session is one conversation.

## Handlers always have a live context

The consequence for you as a user: inside a tool, resource, or prompt handler,
`Vertx.currentContext()` is never `null`, and it is the same context for the whole request.

```java
builder.tool(tool, (exchange, request) -> {
  Context ctx = Vertx.currentContext();  // never null
  ctx.put("tool", request.name());       // survives to the end of the request
  return upstream.call(request)
      .map(response -> {
        // still the same context, still the same data
        assert Vertx.currentContext() == ctx;
        assert "tool".equals(ctx.get("tool"));
        return toResult(response);
      });
});
```

Vert.x 5 also has typed context locals, registered through
`io.vertx.core.spi.context.storage.ContextLocal`, which are what the tracing and instrumentation
integrations use:

```java
static final ContextLocal<String> TENANT = ContextLocal.registerLocal(String.class);

// in a handler
Vertx.currentContext().putLocal(TENANT, tenantId);
String tenant = Vertx.currentContext().getLocal(TENANT);
```

That is what keeps the following working:

- **Context data and context locals.** `Context.put` / `get`, `Context.putLocal` / `getLocal`, and
  anything built on them.
- **Tracing propagation.** Vert.x's OpenTelemetry and Zipkin integrations hang the active span off
  the context. A handler that loses the context loses the span, and the downstream call it makes
  starts a new, unparented trace.
- **Ordered execution.** Work you schedule with `context.runOnContext(...)` runs in order and after
  the current task, not concurrently with it.
- **Vert.x clients.** A `WebClient`, `Pool`, or `EventBus` call made from a live context inherits it
  for the response, so the whole chain stays put.

Transport metadata — headers, an authenticated principal, a tenant id — reaches handlers explicitly
as `exchange.transportContext()`, not through a thread-local or a subscriber context. Since the
context is live, you are free to copy anything from it into the context data yourself if you want
downstream code to pick it up implicitly.

## What goes wrong under a Reactor-based core

The obvious alternative is to keep the official SDK's Reactor-based protocol core and write Vert.x
transports underneath it, bridging `Mono` to `Future`. That does not work, and the reason is
specific rather than aesthetic.

**Reactor has no notion of a Vert.x `Context`.** A `Mono` continuation runs on whatever thread
completed the upstream signal, or on whatever `Scheduler` an operator moved it to. Neither of those
carries a Vert.x context.

Concretely:

```java
// Inside a Reactor-based tool handler:
return Mono.fromCallable(() -> jdbc.query(sql))
    .subscribeOn(Schedulers.boundedElastic())
    .map(this::toResult);
```

The `map` runs on a `boundedElastic` thread. On that thread:

- `Vertx.currentContext()` returns `null`.
- `context.getLocal("requestId")` cannot be reached — there is no context to ask.
- The active span is gone, so any downstream HTTP call is an orphan trace.
- A subsequent Vert.x client call creates or attaches a *different* context, so ordering guarantees
  relative to the rest of the request no longer hold.

You can write a custom `Scheduler` that dispatches onto a Vert.x context, and a `Mono` ⟷ `Future`
bridge at the SDK boundary. Both are real and both help. But the seam leaks in exactly the place you
cannot control: **user code**. One `subscribeOn(boundedElastic())` inside somebody's tool handler —
which is normal, idiomatic Reactor, and which no reviewer would flag — walks straight past the
bridge, and every property above quietly stops holding. There is no compile-time signal and usually
no test failure; you find out from a trace with a hole in it, or from a context-local that is
mysteriously null in production and fine in the unit test.

Failures of that shape are worse than loud ones. This SDK's answer is to not have the seam: the
protocol core is `Future`-shaped all the way down, so there is no scheduler to hop to in the first
place.

The one thing kept from upstream is `McpSchema` — the wire DTOs, which import zero Reactor classes.
`reactor-core` is excluded outright in the POM, so it is not on the compile classpath at all and
cannot creep back in through a transitive dependency.

## Doing blocking work correctly

The rule is the usual Vert.x rule: never block the event loop. What this SDK adds is that blocking
is always explicit at the registration site, so a reader can see which handlers are which.

### `toolBlocking` and friends

The simplest option. Register the handler as blocking and write ordinary synchronous code:

```java
builder.toolBlocking(jdbcTool, (exchange, request) -> {
  // Worker thread. Blocking JDBC, file I/O, and legacy synchronous libraries are all fine here.
  return dao.query(String.valueOf(request.arguments().get("q")));
});
```

Under the hood that is:

```java
public Builder toolBlocking(McpSchema.Tool tool, McpServerFeatures.BlockingToolHandler handler) {
  return tool(tool, (exchange, request) -> this.vertx.executeBlocking(() -> handler.handle(exchange, request)));
}
```

The handler runs on a Vert.x worker thread, so the event loop is never parked, and the returned
future completes back on the calling context — which means anything you chain after it is on the
event loop again, with the context intact.

`resourceBlocking` and `promptBlocking` are the same shape. The blocking handler interfaces declare
`throws Exception`, so you do not have to wrap checked exceptions; a throw becomes a failed future
and then a JSON-RPC error.

### `executeBlocking` inside an async handler

When only part of a handler blocks, keep it async and isolate the blocking part:

```java
builder.tool(mixedTool, (exchange, request) -> exchange.progress(0.0, 2.0, "fetching")
    .compose(v -> webClient.get(443, "api.example.com", "/data").send())          // non-blocking
    .compose(response -> vertx.executeBlocking(() -> heavyParse(response.body()))) // worker thread
    .compose(parsed -> exchange.progress(1.0, 2.0, "storing").map(parsed))
    .map(parsed -> McpSchema.CallToolResult.builder().addTextContent(parsed).build()));
```

`executeBlocking` is ordered by default: tasks submitted from one context run one at a time. Pass
`false` as the second argument for a task that should run concurrently with other blocking work —
but note that a task which never returns *must* be unordered, or it starves everything queued behind
it. That is exactly why `StdioServerTransport` starts its read loop with
`vertx.executeBlocking(..., false)`.

Never call `.result()`, `.toCompletionStage().get()`, `Thread.sleep`, or a synchronous JDBC call
directly in an async handler. Vert.x's blocked-thread checker will log a warning, and under load the
event loop stops serving every other session on it.

### Virtual threads

If you run on a JDK with virtual threads and deploy virtual-thread verticles
(`ThreadingModel.VIRTUAL_THREAD`), you can write blocking-looking code directly in an async handler,
because `Future.await()` parks the virtual thread rather than the carrier:

```java
vertx.deployVerticle(() -> new VerticleBase() {
  @Override
  public Future<?> start() {
    VertxMcpServer server = VertxMcpServer.builder(vertx)
        .tool(tool, (exchange, request) -> {
          // On a virtual-thread verticle, await() parks the virtual thread. The context
          // is preserved across the park, so everything above still holds.
          exchange.progress(0.5, 1.0, "querying").await();
          String answer = pool.preparedQuery(SQL).execute().await().iterator().next().getString(0);
          return Future.succeededFuture(
              McpSchema.CallToolResult.builder().addTextContent(answer).build());
        })
        .build();
    ...
  }
}, new DeploymentOptions().setThreadingModel(ThreadingModel.VIRTUAL_THREAD));
```

Two caveats. `await()` is only legal on a virtual-thread context — calling it on an event-loop
context throws. And virtual threads do not make genuinely blocking native calls cheap: a synchronous
JDBC driver pins the carrier thread, so `toolBlocking` remains the right answer for that.

The SDK itself does not require virtual threads and does not use them internally. It works the same
either way, because the mechanism is the Vert.x context, not the thread.

## Where hops actually happen

For completeness, the places the SDK deliberately crosses a thread boundary:

| Location | Hop | Why |
|---|---|---|
| `Contexts.on` in every session entry point | onto the session context, inline when already there | Serializes session state |
| `toolBlocking` / `resourceBlocking` / `promptBlocking` | onto a worker, back to the caller's context | Explicit blocking |
| `StdioServerTransport.start()` | onto an unordered worker | `System.in` has no non-blocking API |
| `StdioServerTransport` outbound writes | onto an ordered worker | A slow or full pipe must not park the event loop, and frames must stay in order |
| `StdioClientTransport` readers | onto an unordered worker pool of two | stdout and stderr must be drained concurrently, or a full stderr pipe deadlocks the child |
| `StdioClientTransport` writer | onto an ordered worker pool of one | Ordering, and no lock needed |

That is the complete list. Nothing else moves off the context it started on.

## Backpressure

Context pinning handles ordering; backpressure is a separate concern, and `SseWriter` handles it.

An `HttpServerResponse` is a `WriteStream`. When its queue is full, `SseWriter` does not drop the
message and does not block a thread — it registers a `drainHandler` and completes the write's future
only once the write actually goes through:

```java
private Future<Void> writeBackpressured(Buffer buffer) {
  if (this.response.ended() || this.response.closed()) {
    return Future.failedFuture(new IllegalStateException("SSE stream is closed"));
  }
  if (!this.response.writeQueueFull()) {
    return this.response.write(buffer);
  }
  Promise<Void> promise = Promise.promise();
  this.response.drainHandler(v -> {
    this.response.drainHandler(null);
    this.response.write(buffer).onComplete(promise);
  });
  return promise.future();
}
```

Because `exchange.progress(...)` returns that future, a handler that composes on it is naturally
paced by the client. A slow client slows the producer down instead of growing an unbounded queue in
the JVM.

Writes are also chained through a single pending future, mutated only on the writer's context, so
two concurrent `sendMessage` calls cannot interleave halves of two frames on the same response. A
failed write does not poison the chain for the messages behind it — the caller sees the real
failure, but the tail kept for the next write is the recovered one.

The client side is symmetric: the SSE read path is demand-driven, one chunk fetched at a time and
withheld until the consumer has taken every event, so a slow consumer stops the socket rather than
growing a queue.
