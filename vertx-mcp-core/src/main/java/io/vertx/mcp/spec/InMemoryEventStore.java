package io.vertx.mcp.spec;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Future;

/**
 * A bounded in-memory {@link EventStore}. Each stream keeps at most {@code maxPerStream}
 * recent messages; older ones are dropped, and a client that was disconnected long enough
 * to fall off the window gets a failed replay rather than a silently incomplete one.
 *
 * <p>
 * Single-node only. Fine for a sidecar or an embedded MCP server; not fine for a
 * horizontally scaled deployment without sticky routing.
 */
public class InMemoryEventStore implements EventStore {

  private static final int DEFAULT_MAX_PER_STREAM = 256;

  private final int maxPerStream;

  private final AtomicLong counter = new AtomicLong();

  private final Map<String, Deque<Entry>> streams = new ConcurrentHashMap<>();

  /** eventId -> streamId, so a replay can find its stream in O(1). */
  private final Map<String, String> eventToStream = new ConcurrentHashMap<>();

  public InMemoryEventStore() {
    this(DEFAULT_MAX_PER_STREAM);
  }

  public InMemoryEventStore(int maxPerStream) {
    if (maxPerStream < 1) {
      throw new IllegalArgumentException("maxPerStream must be >= 1");
    }
    this.maxPerStream = maxPerStream;
  }

  @Override
  public Future<String> append(String streamId, JSONRPCMessage message) {
    String eventId = streamId + "_" + this.counter.incrementAndGet();
    Deque<Entry> deque = this.streams.computeIfAbsent(streamId, k -> new ArrayDeque<>());
    synchronized (deque) {
      deque.addLast(new Entry(eventId, message));
      while (deque.size() > this.maxPerStream) {
        Entry evicted = deque.removeFirst();
        this.eventToStream.remove(evicted.eventId());
      }
    }
    this.eventToStream.put(eventId, streamId);
    return Future.succeededFuture(eventId);
  }

  @Override
  public Future<String> replayAfter(String lastEventId, BiFunction<String, JSONRPCMessage, Future<Void>> sender) {
    String streamId = this.eventToStream.get(lastEventId);
    if (streamId == null) {
      // Either a bogus id, or the client fell off the retention window. Both are
      // unrecoverable: the client must start a fresh stream rather than silently miss
      // messages.
      return Future.failedFuture(new IllegalArgumentException("Unknown or expired Last-Event-ID: " + lastEventId));
    }
    Deque<Entry> deque = this.streams.get(streamId);
    if (deque == null) {
      return Future.succeededFuture(streamId);
    }
    List<Entry> pending = new ArrayList<>();
    synchronized (deque) {
      boolean found = false;
      for (Entry entry : deque) {
        if (found) {
          pending.add(entry);
        }
        else if (entry.eventId().equals(lastEventId)) {
          found = true;
        }
      }
    }
    return sendSequentially(pending, 0, sender).map(streamId);
  }

  private Future<Void> sendSequentially(List<Entry> entries, int index,
      BiFunction<String, JSONRPCMessage, Future<Void>> sender) {
    if (index >= entries.size()) {
      return Future.succeededFuture();
    }
    Entry entry = entries.get(index);
    return sender.apply(entry.eventId(), entry.message())
      .compose(v -> sendSequentially(entries, index + 1, sender));
  }

  @Override
  public Future<Void> removeStream(String streamId) {
    Deque<Entry> deque = this.streams.remove(streamId);
    if (deque != null) {
      synchronized (deque) {
        deque.forEach(e -> this.eventToStream.remove(e.eventId()));
      }
    }
    return Future.succeededFuture();
  }

  private record Entry(String eventId, JSONRPCMessage message) {
  }
}
