package io.vertx.mcp.server.transport;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.mcp.internal.Contexts;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpTransports.McpStreamableServerTransport;

/**
 * Writes an SSE event stream onto an {@link HttpServerResponse}.
 *
 * <p>
 * Three properties matter here and none of them are free:
 * <ul>
 * <li><b>Backpressure.</b> The response is a {@code WriteStream}. When its queue is full
 * the returned future does not complete until {@code drainHandler} fires and the write
 * actually goes through -- messages are never dropped and no thread is ever blocked. A
 * slow client therefore slows the producer down instead of growing an unbounded queue in
 * the JVM.</li>
 * <li><b>Serialization.</b> Two concurrent {@code sendMessage} calls must not interleave
 * halves of two frames on the same response. Writes are chained through a single pending
 * future, and the chain is only ever mutated on the Vert.x context this writer was
 * created on.</li>
 * <li><b>Idempotent close.</b> {@code response.end()} is called exactly once, after
 * everything already queued has drained.</li>
 * </ul>
 *
 * <p>
 * Frames follow the SSE grammar: an optional {@code id:} line (present when the caller
 * supplied an event id, which is what makes {@code Last-Event-ID} resumption possible), an
 * {@code event:} line, then one {@code data:} line per line of payload. The codec emits
 * compact single-line JSON, but the split is done anyway because a newline inside the
 * payload would otherwise terminate the frame early.
 */
public final class SseWriter implements McpStreamableServerTransport {

  private static final Logger logger = LoggerFactory.getLogger(SseWriter.class);

  /** SSE event type carrying a JSON-RPC message. */
  public static final String MESSAGE_EVENT = "message";

  /** SSE event type carrying the POST endpoint URL, used by the legacy transport. */
  public static final String ENDPOINT_EVENT = "endpoint";

  private final HttpServerResponse response;

  private final JsonRpcCodec codec;

  private final Context context;

  /** Tail of the serialized write chain. Only read or written on {@link #context}. */
  private Future<Void> pending = Future.succeededFuture();

  /** Run once when the writer transitions to closed. Only touched on {@link #context}. */
  private final List<Runnable> closeHooks = new ArrayList<>();

  private volatile boolean closed;

  public SseWriter(Vertx vertx, HttpServerResponse response, JsonRpcCodec codec) {
    this.response = response;
    this.codec = codec;
    this.context = Contexts.current(vertx);
  }

  @Override
  public Future<Void> sendMessage(JSONRPCMessage message, String messageId) {
    return sendEvent(MESSAGE_EVENT, this.codec.encode(message), messageId);
  }

  /** Sends an arbitrary SSE event. {@code eventId} may be {@code null}. */
  public Future<Void> sendEvent(String eventType, String data, String eventId) {
    return enqueue(frame(eventType, data, eventId));
  }

  /**
   * Sends a comment line. Invisible to an SSE parser, but it puts bytes on the wire --
   * which is how a keep-alive stops a proxy from reaping an idle connection, and how the
   * response head gets flushed so the client observes the 200 immediately.
   */
  public Future<Void> sendComment(String text) {
    return enqueue(Buffer.buffer(": " + text + "\n\n"));
  }

  /** Whether this writer has been closed, or the client hung up. */
  public boolean isClosed() {
    return this.closed || this.response.ended() || this.response.closed();
  }

  /**
   * Registers a callback to run exactly once when this writer closes, whichever way it
   * closes. Used to cancel keep-alive timers and unbind session streams.
   */
  public void onClose(Runnable hook) {
    Contexts.run(this.context, () -> {
      if (this.closed) {
        hook.run();
      }
      else {
        this.closeHooks.add(hook);
      }
    });
  }

  /**
   * Marks the writer closed without touching the response. For when the client
   * disconnected: there is nothing left to end, and calling {@code end()} on a dead
   * connection just raises noise.
   */
  public void markClosed() {
    Contexts.run(this.context, () -> {
      if (!this.closed) {
        this.closed = true;
        runCloseHooks();
      }
    });
  }

  @Override
  public Future<Void> close() {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      runCloseHooks();
      // Flush whatever is still queued before ending, otherwise the last response frame
      // can be truncated.
      return this.pending.compose(v -> end(), err -> end());
    });
  }

  private void runCloseHooks() {
    List<Runnable> hooks = new ArrayList<>(this.closeHooks);
    this.closeHooks.clear();
    for (Runnable hook : hooks) {
      try {
        hook.run();
      }
      catch (RuntimeException e) {
        logger.debug("SSE close hook failed", e);
      }
    }
  }

  private Future<Void> end() {
    if (this.response.ended() || this.response.closed()) {
      return Future.succeededFuture();
    }
    try {
      return this.response.end().recover(err -> {
        logger.debug("Ignoring error ending SSE response", err);
        return Future.succeededFuture();
      });
    }
    catch (RuntimeException e) {
      logger.debug("Ignoring error ending SSE response", e);
      return Future.succeededFuture();
    }
  }

  private Future<Void> enqueue(Buffer buffer) {
    return Contexts.on(this.context, () -> {
      if (isClosed()) {
        return Future.failedFuture(new IllegalStateException("SSE stream is closed"));
      }
      Future<Void> write = this.pending.compose(v -> writeBackpressured(buffer),
          err -> writeBackpressured(buffer));
      // A failed write must not poison the chain for the messages behind it, so the tail
      // we keep is the recovered one; the caller still sees the real failure.
      this.pending = write.recover(err -> Future.succeededFuture());
      return write;
    });
  }

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

  private static Buffer frame(String eventType, String data, String eventId) {
    StringBuilder sb = new StringBuilder(data.length() + 48);
    if (eventId != null) {
      sb.append("id: ").append(eventId).append('\n');
    }
    sb.append("event: ").append(eventType).append('\n');
    int start = 0;
    while (true) {
      int newline = data.indexOf('\n', start);
      int end = newline < 0 ? data.length() : newline;
      // A CR before the LF belongs to the line terminator, not to the payload.
      int trimmed = end > start && data.charAt(end - 1) == '\r' ? end - 1 : end;
      sb.append("data: ").append(data, start, trimmed).append('\n');
      if (newline < 0) {
        break;
      }
      start = newline + 1;
    }
    sb.append('\n');
    return Buffer.buffer(sb.toString());
  }
}
