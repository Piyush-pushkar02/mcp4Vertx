package io.vertx.mcp.client.transport;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import io.vertx.core.buffer.Buffer;

/**
 * An incremental parser for the {@code text/event-stream} wire format, per the
 * <a href="https://html.spec.whatwg.org/multipage/server-sent-events.html">WHATWG
 * server-sent events</a> grammar.
 *
 * <p>
 * It is fed arbitrary {@link Buffer} chunks straight off the socket, so a chunk boundary
 * may fall anywhere -- in the middle of a field name, between the {@code CR} and the
 * {@code LF} of a {@code CRLF}, or between the two blank-line newlines that terminate an
 * event. Everything that is not yet a complete line stays in {@link #pending} until more
 * bytes arrive.
 *
 * <p>
 * Splitting on the {@code CR}/{@code LF} bytes before decoding is safe for UTF-8: every
 * byte of a multi-byte sequence has its high bit set, so a line terminator can never be
 * mistaken for part of a character.
 *
 * <p>
 * Not thread-safe. Each instance belongs to one response stream and is only ever written
 * to from that stream's Vert.x context.
 */
final class SseParser {

  /**
   * Deliberately generous cap on a single line. It exists only so a server that never
   * sends a newline cannot grow the heap without bound.
   */
  private static final int MAX_LINE_BYTES = 1 << 20;

  private final Consumer<SseEvent> handler;

  private Buffer pending = Buffer.buffer();

  private final StringBuilder data = new StringBuilder();

  private String eventType;

  private String lastEventId;

  private Long retryMillis;

  private boolean ended;

  SseParser(Consumer<SseEvent> handler) {
    this.handler = handler;
  }

  /** The most recent {@code id:} the server sent, for {@code Last-Event-ID} on reconnect. */
  String lastEventId() {
    return this.lastEventId;
  }

  /** The server's suggested reconnection delay, or {@code null} if it never sent one. */
  Long retryMillis() {
    return this.retryMillis;
  }

  /** Feeds one chunk. Any complete events it completes are dispatched before returning. */
  void write(Buffer chunk) {
    if (this.ended || chunk == null || chunk.length() == 0) {
      return;
    }
    this.pending = this.pending.length() == 0 ? chunk.copy() : this.pending.appendBuffer(chunk);
    drain(false);
  }

  /**
   * Signals end of stream.
   *
   * <p>
   * The specification says a truncated final event is discarded. We flush it instead:
   * plenty of MCP servers close the response right after writing the last {@code data:}
   * line without the terminating blank line, and dropping the JSON-RPC result that was
   * already fully received would strand the caller's request until it times out.
   */
  void end() {
    if (this.ended) {
      return;
    }
    drain(true);
    if (this.pending.length() > 0) {
      handleLine(this.pending.toString(StandardCharsets.UTF_8));
      this.pending = Buffer.buffer();
    }
    this.ended = true;
    dispatch();
  }

  private void drain(boolean atEnd) {
    int length = this.pending.length();
    int start = 0;
    int i = 0;
    while (i < length) {
      byte b = this.pending.getByte(i);
      if (b == '\n') {
        handleLine(this.pending.getString(start, i, "UTF-8"));
        start = ++i;
      }
      else if (b == '\r') {
        // A trailing CR may be the first half of a CRLF whose LF is in the next chunk;
        // consuming it now would fabricate an extra empty line and dispatch too early.
        if (i == length - 1 && !atEnd) {
          break;
        }
        handleLine(this.pending.getString(start, i, "UTF-8"));
        i++;
        if (i < length && this.pending.getByte(i) == '\n') {
          i++;
        }
        start = i;
      }
      else {
        i++;
      }
    }
    this.pending = start == 0 ? this.pending : this.pending.getBuffer(start, length);
    if (this.pending.length() > MAX_LINE_BYTES) {
      throw new IllegalStateException("SSE line exceeded " + MAX_LINE_BYTES + " bytes without a terminator");
    }
  }

  private void handleLine(String line) {
    if (line.isEmpty()) {
      dispatch();
      return;
    }
    if (line.charAt(0) == ':') {
      // Comment. Servers use these as keep-alives; nothing to do but stay connected.
      return;
    }
    int colon = line.indexOf(':');
    String field;
    String value;
    if (colon < 0) {
      field = line;
      value = "";
    }
    else {
      field = line.substring(0, colon);
      value = line.substring(colon + 1);
      if (!value.isEmpty() && value.charAt(0) == ' ') {
        value = value.substring(1);
      }
    }
    switch (field) {
      case "data" -> this.data.append(value).append('\n');
      case "event" -> this.eventType = value;
      case "id" -> {
        // The spec requires ignoring an id containing a NUL rather than storing it.
        if (value.indexOf('\0') < 0) {
          this.lastEventId = value;
        }
      }
      case "retry" -> {
        try {
          this.retryMillis = Long.parseLong(value);
        }
        catch (NumberFormatException ignored) {
          // Not a valid integer: the spec says ignore the field entirely.
        }
      }
      default -> {
        // Unknown field: ignore, per spec.
      }
    }
  }

  private void dispatch() {
    if (this.data.length() == 0) {
      // A blank line with no accumulated data only resets the event type; it is not an
      // event. This is what makes keep-alive newlines harmless.
      this.eventType = null;
      return;
    }
    // The data buffer always ends with the newline appended by the last data: field.
    this.data.setLength(this.data.length() - 1);
    String payload = this.data.toString();
    String type = this.eventType != null && !this.eventType.isEmpty() ? this.eventType : SseEvent.DEFAULT_EVENT;
    this.data.setLength(0);
    this.eventType = null;
    this.handler.accept(new SseEvent(this.lastEventId, type, payload));
  }
}
