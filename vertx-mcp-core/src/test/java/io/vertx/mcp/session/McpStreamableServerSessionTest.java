package io.vertx.mcp.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mcp.spec.InMemoryEventStore;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpTransports.McpStreamableServerTransport;

/**
 * Establishes the two properties that make {@code Last-Event-ID} resumption trustworthy:
 * nothing is lost while the client is disconnected, and nothing is retained forever once
 * it can no longer be useful.
 */
@ExtendWith(VertxExtension.class)
class McpStreamableServerSessionTest {

  /** Captures what was written and with which SSE event id. */
  private static final class RecordingTransport implements McpStreamableServerTransport {

    final List<String> eventIds = new ArrayList<>();

    final List<JSONRPCMessage> messages = new ArrayList<>();

    @Override
    public Future<Void> sendMessage(JSONRPCMessage message, String messageId) {
      this.messages.add(message);
      this.eventIds.add(messageId);
      return Future.succeededFuture();
    }

    @Override
    public Future<Void> close() {
      return Future.succeededFuture();
    }

    List<String> methods() {
      return this.messages.stream().map(message -> ((JSONRPCNotification) message).method()).toList();
    }
  }

  private static McpStreamableServerSession session(Vertx vertx, InMemoryEventStore store) {
    return new McpStreamableServerSession(vertx, "sess", JsonRpcCodec.defaultCodec(), Duration.ofSeconds(5), Map.of(),
        Map.of(), store);
  }

  @Test
  void replaysNotificationsEmittedWhileNoListeningStreamWasBound(Vertx vertx, VertxTestContext testContext) {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingTransport connected = new RecordingTransport();
    RecordingTransport resumed = new RecordingTransport();

    vertx.runOnContext(ignored -> {
      McpStreamableServerSession session = session(vertx, store);
      session.listeningStream(connected)
        .compose(stream -> session.sendNotification(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, Map.of())
          // The client's GET drops here. Anything the server emits next must survive.
          .compose(sent -> stream.close()))
        .compose(closed -> session.sendNotification(McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED, Map.of()))
        .compose(sent -> session.sendNotification(McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED, Map.of()))
        .compose(sent -> session.resumeStream(resumed, connected.eventIds.get(0)))
        .onComplete(testContext.succeeding(stream -> testContext.verify(() -> {
          assertThat(connected.methods()).containsExactly(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED);
          assertThat(resumed.methods()).containsExactly(McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED,
              McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED);
          assertThat(resumed.eventIds).doesNotContainNull();
          testContext.completeNow();
        })));
    });
  }

  @Test
  void succeedsSilentlyWhenThereIsNoListeningStreamAtAll(Vertx vertx, VertxTestContext testContext) {
    InMemoryEventStore store = new InMemoryEventStore();
    vertx.runOnContext(ignored -> session(vertx, store)
      .sendNotification(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, Map.of())
      .onComplete(testContext.succeeding(v -> testContext.verify(() -> {
        // Recorded for a later resume rather than failing -- broadcast callers would only
        // swallow the failure, which is how such notifications go missing unnoticed.
        assertThat(store.replayAfter("sess-listening_1", (id, message) -> Future.succeededFuture()).succeeded())
          .isTrue();
        testContext.completeNow();
      }))));
  }

  @Test
  void purgesRecordedEventsWhenTheSessionEnds(Vertx vertx, VertxTestContext testContext) {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingTransport transport = new RecordingTransport();

    vertx.runOnContext(ignored -> {
      McpStreamableServerSession session = session(vertx, store);
      session.listeningStream(transport)
        .compose(stream -> session.sendNotification(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, Map.of()))
        .compose(sent -> session.close())
        .compose(closed -> store.replayAfter(transport.eventIds.get(0), (id, message) -> Future.succeededFuture())
          .map(streamId -> "retained")
          .otherwise(err -> "purged"))
        .onComplete(testContext.succeeding(outcome -> testContext.verify(() -> {
          assertThat(outcome).isEqualTo("purged");
          testContext.completeNow();
        })));
    });
  }
}
