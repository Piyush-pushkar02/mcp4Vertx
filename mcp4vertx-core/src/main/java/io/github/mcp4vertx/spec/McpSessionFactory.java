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

import io.github.mcp4vertx.session.McpServerSession;
import io.github.mcp4vertx.spec.McpTransports.McpServerTransport;

/**
 * Creates a session bound to a freshly connected client. Implemented by the server layer;
 * called by transport providers.
 */
@FunctionalInterface
public interface McpSessionFactory {

  McpServerSession create(String sessionId, McpServerTransport transport);
}
