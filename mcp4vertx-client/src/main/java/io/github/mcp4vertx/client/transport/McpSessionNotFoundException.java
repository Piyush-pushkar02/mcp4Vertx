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
 * The server no longer knows the {@code Mcp-Session-Id} we are presenting -- it answered
 * {@code 404 Not Found}.
 *
 * <p>
 * This is separated from every other transport failure because the recovery is specific
 * and mechanical: per the streamable HTTP specification the client must start a new
 * session by sending {@code initialize} again, without the stale session header. Callers
 * catch this type to drive that re-handshake instead of surfacing an error to the user.
 */
public class McpSessionNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String sessionId;

  public McpSessionNotFoundException(String sessionId, String message) {
    super(message);
    this.sessionId = sessionId;
  }

  /** The session id the server rejected, or {@code null} if we had not been issued one. */
  public String sessionId() {
    return this.sessionId;
  }
}
