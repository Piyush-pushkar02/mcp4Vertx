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
 * The two {@code Content-Type} tests every MCP HTTP response is dispatched on.
 *
 * <p>
 * Both are prefix matches on the lower-cased header because servers routinely append
 * parameters -- {@code text/event-stream;charset=UTF-8} is the same media type as
 * {@code text/event-stream}, and refusing it would break interoperability for no reason.
 */
final class MediaTypes {

  private MediaTypes() {
  }

  /** Whether the body is a Server-Sent Events stream and must be parsed incrementally. */
  static boolean isEventStream(String contentType) {
    return contentType != null && contentType.toLowerCase().startsWith("text/event-stream");
  }

  /** Whether the body is a single JSON-RPC message that can simply be collected. */
  static boolean isJson(String contentType) {
    return contentType != null && contentType.toLowerCase().startsWith("application/json");
  }
}
