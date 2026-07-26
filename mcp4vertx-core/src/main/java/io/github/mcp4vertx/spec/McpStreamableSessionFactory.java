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

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.github.mcp4vertx.session.McpStreamableServerSession;

/**
 * Creates a streamable-HTTP session in response to an {@code initialize} request.
 *
 * <p>
 * Session creation is a two-step dance because the initialize response has to go out
 * before the session is considered live: the factory returns both the session and the
 * result to write back.
 */
@FunctionalInterface
public interface McpStreamableSessionFactory {

  Future<InitResult> startSession(McpSchema.InitializeRequest initializeRequest);

  /**
   * @param session the newly created session, already registered
   * @param initializeResult the payload to write back to the client
   */
  record InitResult(McpStreamableServerSession session, McpSchema.InitializeResult initializeResult) {
  }
}
