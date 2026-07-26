package io.vertx.mcp.spec;

import java.util.function.BiFunction;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Future;

/**
 * Backs SSE resumability. Every message written to a stream is recorded against a
 * monotonic event id; when a client reconnects with {@code Last-Event-ID} the store
 * replays what it missed.
 *
 * <p>
 * The default {@link InMemoryEventStore} is fine for a single node. Behind a load
 * balancer without sticky sessions you want a shared implementation -- Redis, or a Vert.x
 * cluster-wide {@code AsyncMap}.
 */
public interface EventStore {

  /**
   * Records a message and returns the event id to put in the SSE {@code id:} field.
   * Implementations may return {@code null} to mean "not stored, do not emit an id".
   */
  Future<String> append(String streamId, JSONRPCMessage message);

  /**
   * Replays everything recorded after {@code lastEventId} on that event's stream.
   *
   * @param sender invoked per replayed message with (eventId, message); replay stops if
   * it fails
   * @return the stream id the events belonged to, so the caller can rebind its transport
   */
  Future<String> replayAfter(String lastEventId, BiFunction<String, JSONRPCMessage, Future<Void>> sender);

  /** Drops all state for a stream. Called when a stream closes for good. */
  Future<Void> removeStream(String streamId);

  /** A store that keeps nothing. Resumability is then simply unavailable. */
  static EventStore noop() {
    return new EventStore() {
      @Override
      public Future<String> append(String streamId, JSONRPCMessage message) {
        return Future.succeededFuture(null);
      }

      @Override
      public Future<String> replayAfter(String lastEventId,
          BiFunction<String, JSONRPCMessage, Future<Void>> sender) {
        return Future.failedFuture(new UnsupportedOperationException("Event replay is not enabled on this server"));
      }

      @Override
      public Future<Void> removeStream(String streamId) {
        return Future.succeededFuture();
      }
    };
  }
}
