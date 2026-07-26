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
import io.github.mcp4vertx.McpTransportContext;

/**
 * Handles a single request or notification in the stateless transport, where there is no
 * session to route through.
 */
public interface McpStatelessHandler {

  Future<McpSchema.JSONRPCResponse> handleRequest(McpTransportContext context, McpSchema.JSONRPCRequest request);

  Future<Void> handleNotification(McpTransportContext context, McpSchema.JSONRPCNotification notification);
}
