/*
 * Copyright 2026 the mcp4vertx authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.mcp4vertx.client.transport;

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
