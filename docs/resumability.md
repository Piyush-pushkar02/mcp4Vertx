---
title: Resumability and scaling
nav_order: 6
---

# Resumability and scaling

A Streamable HTTP client keeps a long-lived `GET /mcp` stream open to receive unsolicited
server-to-client traffic. Sockets drop: laptops sleep, load balancers recycle idle connections,
mobile networks change. Resumability is how a client picks up where it left off instead of silently
missing whatever the server said while it was away.

**The library has no infrastructure dependency for this.** `EventStore` is a four-method interface
with an in-memory default. You do not need Redis, you do not need a database, and for most
deployments you do not need to implement it at all — see [Scaling beyond one
node](#scaling-beyond-one-node).

## How it works end to end

### Event ids

Every message written to a stream is recorded against a monotonic event id, which the SSE writer
emits in the frame's `id:` field:

```
id: 3f8c2a1e-...-listening_7
event: message
data: {"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}

```

An id is opaque to the client. `InMemoryEventStore` generates `streamId + "_" + counter`, which is
convenient for debugging but not part of any contract; your own implementation can use whatever it
likes as long as ids are unique and an id identifies its stream.

A session has more than one stream:

| Stream | Stream id | Carries |
|---|---|---|
| Listening stream | `<sessionId>-listening` | Unsolicited server-to-client traffic |
| Response stream | `<sessionId>-<requestId>` | One request's progress, logs, reverse requests, and result |

### Reconnecting

The browser SSE convention, which MCP adopts: the client remembers the last `id:` it saw and sends
it as `Last-Event-ID` when it reopens the stream.

```
GET /mcp HTTP/1.1
Accept: text/event-stream
Mcp-Session-Id: 3f8c2a1e-...
Last-Event-ID: 3f8c2a1e-...-listening_7
```

`StreamableHttpClientTransport` does this for you: it tracks the last id seen on the standalone
stream and replays it on every reconnect, with exponential backoff (`reconnectDelay`, doubling, up
to `maxReconnectDelay`, for `maxReconnectAttempts` tries). A `retry:` field from the server
overrides the base delay. Any traffic arriving resets the backoff ladder.

On the server, `McpStreamableServerSession.resumeStream(...)` asks the store to replay everything
after that id onto the new transport, then binds the new transport as the listening stream.

### Resumption is best-effort

An unknown or expired `Last-Event-ID` costs the client its replay, not its stream:

```java
stream = session.resumeStream(writer, lastEventId).recover(err -> {
  logger.warn("Cannot replay session {} from Last-Event-ID {}; opening a fresh listening stream",
      sessionId, lastEventId, err);
  return session.listeningStream(writer);
});
```

The client gets a working `200` stream either way. That is deliberate: a client that has been away
too long is better served by a live stream plus a re-list than by a `404` it has to recover from.

### Traffic emitted while the client is away is still recorded

This is the property that makes resumption worth having, and the easy thing to get wrong. When the
server broadcasts a notification and there is no listening stream bound, it is recorded against the
listening stream's id instead of being discarded:

```java
@Override
protected Future<Void> write(JSONRPCMessage message) {
  SessionStream stream = this.listeningStream;
  if (stream == null) {
    return this.eventStore.append(listeningStreamId(), message).mapEmpty();
  }
  return stream.write(message);
}
```

Dropping it here instead — the naive implementation — loses `tools/list_changed` and
`resources/updated` across a reconnect window, *silently*, because broadcast callers treat send
failures as non-fatal. The test suite pins this down in both directions: notifications emitted while
no stream was bound are replayed on reconnect, and a send with no stream at all succeeds rather than
failing.

Server-to-client *requests* are different. `sendRequest` fails immediately when there is no
listening stream, because a request with nobody to answer it is not something to queue — the caller
needs to know now.

### Records are purged when they can no longer be useful

Two purge points, so a long-lived server does not leak proportionally to total requests served:

- When a response stream's result reaches the client, that stream's records are dropped. Nothing on
  it is worth resuming. If the write *failed*, the records are kept, because the client may still
  come back with `Last-Event-ID`.
- When a session ends, every stream it owned is purged, including the listening stream's id — which
  may have unbound while still holding records, so it is purged by id rather than only by live
  stream.

## The `EventStore` SPI

```java
public interface EventStore {

  /**
   * Records a message and returns the event id to put in the SSE {@code id:} field.
   * Implementations may return {@code null} to mean "not stored, do not emit an id".
   */
  Future<String> append(String streamId, JSONRPCMessage message);

  /**
   * Replays everything recorded after {@code lastEventId} on that event's stream.
   * @return the stream id the events belonged to, so the caller can rebind its transport
   */
  Future<String> replayAfter(String lastEventId, BiFunction<String, JSONRPCMessage, Future<Void>> sender);

  /** Drops all state for a stream. Called when a stream closes for good. */
  Future<Void> removeStream(String streamId);

  /** A store that keeps nothing. Resumability is then simply unavailable. */
  static EventStore noop();
}
```

Four methods. Two of them are one-liners for most implementations.

`replayAfter` must fail when it cannot honor the request — an unknown id, or an id that has fallen
off a retention window. Returning an incomplete replay as if it were complete is the one thing an
implementation must not do, because the client has no way to tell.

Install it on the builder:

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .eventStore(myEventStore)
    .build();
```

## `InMemoryEventStore`

The default. Each stream keeps at most `maxPerStream` recent messages (256 by default) in an
`ArrayDeque`; older ones are evicted.

```java
.eventStore(new InMemoryEventStore(1024))
```

A client disconnected long enough to fall off the window gets a *failed* replay rather than a
silently incomplete one — the id is no longer in the index, so `replayAfter` fails, and the
transport gives it a fresh stream.

Its limitation is exactly one thing: **it is single-node.** Records live in the JVM heap of the
instance that wrote them. A client that reconnects to a different instance finds an id that instance
has never seen, and gets a fresh stream instead of a replay. That is correct behavior, not
corruption — but it means resumption stops working the moment you scale out without doing something
about it.

## Scaling beyond one node

In order of preference.

### 1. Sticky sessions on `Mcp-Session-Id`

**This is the right answer for most deployments.** No store to write, no new infrastructure, no
extra failure mode.

Every Streamable HTTP request after `initialize` carries the session id in the `Mcp-Session-Id`
header. Hash on it at the load balancer, and every request for a session — including the reconnected
`GET` — lands on the instance that owns it. `InMemoryEventStore` then works exactly as designed.

nginx:

```nginx
upstream mcp {
    hash $http_mcp_session_id consistent;
    server mcp-1:8080;
    server mcp-2:8080;
    server mcp-3:8080;
}

server {
    location /mcp {
        proxy_pass http://mcp;
        proxy_http_version 1.1;
        proxy_buffering off;          # SSE must not be buffered
        proxy_read_timeout 1h;        # the listening stream is legitimately idle
    }
}
```

Envoy, with a ring-hash load balancer:

```yaml
route:
  cluster: mcp
  hash_policy:
    - header:
        header_name: mcp-session-id
  timeout: 0s                          # do not time out the SSE stream
```

Kubernetes ingress-nginx:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/upstream-hash-by: "$http_mcp_session_id"
    nginx.ingress.kubernetes.io/proxy-buffering: "off"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
```

Note that `initialize` arrives with no session header, so it hashes to an arbitrary instance — which
is fine, because that instance mints the session and every subsequent request follows the id back to
it.

What sticky routing does not survive is losing the instance. When a pod restarts, its sessions are
gone; the clients get `404` with the session-not-found code, drop their stale ids, and re-initialize.
That is the designed recovery path, and `StreamableHttpClientTransport` surfaces it as
`McpSessionNotFoundException` so you can act on it.

If you are already running sticky sessions, you are done. Skip the rest.

### 2. A cluster-wide `EventStore` over the Vert.x cluster manager

If you cannot get sticky routing — a load balancer you do not control, or a requirement to survive
losing the instance — the next option reuses infrastructure a clustered Vert.x application already
has. `vertx.sharedData().getClusterWideMap(...)` is backed by whatever cluster manager you already
run (Hazelcast, Infinispan, Ignite, Zookeeper). No new dependency, no new thing to operate.

A sketch. It is a sketch and not a shipped class because the retention policy, the eviction policy,
and the serialization format are all deployment decisions:

```java
public final class ClusteredEventStore implements EventStore {

  private final SharedData sharedData;
  private final JsonRpcCodec codec;
  private final AtomicLong counter = new AtomicLong();
  private final String node;

  public ClusteredEventStore(Vertx vertx, JsonRpcCodec codec, String node) {
    this.sharedData = vertx.sharedData();
    this.codec = codec;
    this.node = node;
  }

  /** streamId -> JsonArray of {"id":..., "msg":...} */
  private Future<AsyncMap<String, JsonArray>> events() {
    return this.sharedData.getClusterWideMap("mcp.events");
  }

  /** eventId -> streamId, so a replay can find its stream from the id alone. */
  private Future<AsyncMap<String, String>> index() {
    return this.sharedData.getClusterWideMap("mcp.events.index");
  }

  @Override
  public Future<String> append(String streamId, JSONRPCMessage message) {
    String eventId = this.node + "-" + streamId + "_" + this.counter.incrementAndGet();
    JsonObject entry = new JsonObject()
        .put("id", eventId)
        .put("msg", this.codec.encode(message));
    return events()
        .compose(map -> map.get(streamId).compose(existing -> {
          JsonArray recorded = existing != null ? existing : new JsonArray();
          recorded.add(entry);
          while (recorded.size() > MAX_PER_STREAM) {
            recorded.remove(0);
          }
          return map.put(streamId, recorded);
        }))
        .compose(v -> index().compose(map -> map.put(eventId, streamId)))
        .map(eventId);
  }

  @Override
  public Future<String> replayAfter(String lastEventId,
      BiFunction<String, JSONRPCMessage, Future<Void>> sender) {
    return index().compose(idx -> idx.get(lastEventId)).compose(streamId -> {
      if (streamId == null) {
        // Unknown or evicted: fail rather than replay a partial stream. The transport
        // will fall back to a fresh listening stream.
        return Future.failedFuture(
            new IllegalArgumentException("Unknown or expired Last-Event-ID: " + lastEventId));
      }
      return events().compose(map -> map.get(streamId)).compose(recorded -> {
        List<JsonObject> after = new ArrayList<>();
        boolean found = false;
        for (Object raw : recorded == null ? new JsonArray() : recorded) {
          JsonObject entry = (JsonObject) raw;
          if (found) {
            after.add(entry);
          }
          else if (lastEventId.equals(entry.getString("id"))) {
            found = true;
          }
        }
        return sendSequentially(after, 0, sender).map(streamId);
      });
    });
  }

  @Override
  public Future<Void> removeStream(String streamId) {
    return events().compose(map -> map.get(streamId).compose(recorded -> {
      Future<Void> cleared = map.remove(streamId).mapEmpty();
      if (recorded == null) {
        return cleared;
      }
      return index().compose(idx -> {
        List<Future<?>> removals = new ArrayList<>();
        recorded.forEach(raw -> removals.add(idx.remove(((JsonObject) raw).getString("id"))));
        return Future.all(removals).mapEmpty();
      }).compose(v -> cleared);
    }));
  }

  private Future<Void> sendSequentially(List<JsonObject> entries, int index,
      BiFunction<String, JSONRPCMessage, Future<Void>> sender) {
    if (index >= entries.size()) {
      return Future.succeededFuture();
    }
    JsonObject entry = entries.get(index);
    JSONRPCMessage message;
    try {
      message = this.codec.decode(entry.getString("msg"));
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
    return sender.apply(entry.getString("id"), message)
        .compose(v -> sendSequentially(entries, index + 1, sender));
  }
}
```

Three things to get right when you build this out:

- **Serialization.** An `AsyncMap` value must be a primitive, a `String`, `Serializable`, or
  `ClusterSerializable`. `JsonArray` and `JsonObject` are `ClusterSerializable`, which is why the
  message is stored as its encoded JSON string rather than as a `JSONRPCMessage`.
- **Read-modify-write on append.** The sketch above does a plain `get` then `put`. In practice all
  appends for one stream originate on the node that owns the session, so they are already serialized
  by that session's context; the race only appears across a failover. If you need it airtight, use
  `AsyncMap.replace(key, oldValue, newValue)` in a retry loop, or route appends through the event bus
  to a single owner.
- **Expiry.** Cluster-wide maps do not evict on their own. Bound the per-stream array as above, and
  clean up on `removeStream`. If your cluster manager supports TTL (`put(k, v, ttl)`), use it as a
  backstop against sessions that die without a clean close.

### 3. Accept no resumption

Entirely legal, and the right call when your server's unsolicited traffic is limited to
`list_changed` notifications that a client re-syncs from anyway.

```java
VertxMcpServer server = VertxMcpServer.builder(vertx)
    .eventStore(EventStore.noop())
    .build();
```

`EventStore.noop()` records nothing and emits no `id:` fields, so clients never send
`Last-Event-ID`. A dropped stream is reopened fresh, and the client re-lists. What you give up is
notifications emitted during the gap; what you gain is zero state, which makes every node
interchangeable.

If your server's push traffic is stateless invalidation signals rather than a durable event log,
this is a perfectly good answer and cheaper than either option above.

The stateless transport is the strongest version of this position — no sessions at all. See
[Transports](transports.md#stateless-http).

### 4. An external store, if you already run one

If you already operate Redis, Postgres, or similar, and you already run a sticky-less multi-node
deployment where losing replay is unacceptable, implementing `EventStore` on top of it is
straightforward: a sorted set or an append-only table per stream, a secondary index from event id to
stream id, and a TTL.

This is last on the list on purpose. Adding a datastore to an application that did not otherwise
need one, to recover notifications that a `list_changed` re-sync would have recovered anyway, is a
poor trade. Reach for it when you have an existing store and a concrete requirement, not by default.

## Testing your implementation

The properties worth asserting, all of which the library's own tests assert for
`InMemoryEventStore`:

- Everything recorded after the anchor event is replayed, in order, exactly once.
- The anchor event itself is not re-sent.
- Nothing is replayed that was never recorded.
- Notifications emitted while no stream was bound are replayed on reconnect.
- An unknown or expired id fails the replay rather than returning a partial one.
- Records are gone after `removeStream` and after the session closes.
