package io.vertx.mcp.client.transport;

/**
 * One dispatched Server-Sent Event.
 *
 * @param id the value of the stream's last {@code id:} field at dispatch time, or
 * {@code null} if the server has never sent one. Per the SSE specification this is stream
 * state, not per-event state: it survives events that carry no {@code id:} of their own,
 * which is exactly what makes {@code Last-Event-ID} resumption work.
 * @param event the {@code event:} field, defaulting to {@code "message"}
 * @param data the concatenated {@code data:} fields, joined with {@code \n} and with the
 * trailing newline removed
 */
public record SseEvent(String id, String event, String data) {

  /** The event type SSE assigns when the server sends no explicit {@code event:} field. */
  public static final String DEFAULT_EVENT = "message";
}
