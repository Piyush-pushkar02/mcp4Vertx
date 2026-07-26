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
package io.github.mcp4vertx.spec;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;

/**
 * A live JSON-RPC conversation with a peer. Both server-side and client-side sessions
 * implement this.
 */
public interface McpSession {

  /**
   * Sends a request and completes when the peer answers. The future fails with
   * {@link io.modelcontextprotocol.spec.McpError} if the peer returns a JSON-RPC error, or
   * with {@link java.util.concurrent.TimeoutException} if the configured request timeout
   * elapses first.
   */
  <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType);

  /** Fire-and-forget. Completes once the bytes are accepted by the transport. */
  Future<Void> sendNotification(String method, Object params);

  default Future<Void> sendNotification(String method) {
    return sendNotification(method, null);
  }

  /** Closes the session and fails any in-flight requests. */
  Future<Void> close();

  /**
   * Whether a {@code notifications/message} at this level should be delivered, per the
   * client's most recent {@code logging/setLevel}.
   */
  default boolean isLoggingLevelEnabled(McpSchema.LoggingLevel level) {
    return true;
  }

  /** Records the client's requested minimum logging level. */
  default void setMinLoggingLevel(McpSchema.LoggingLevel level) {
  }
}
