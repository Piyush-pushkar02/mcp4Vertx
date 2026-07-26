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
package io.github.mcp4vertx.server;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.spec.McpErrors;
import io.github.mcp4vertx.spec.McpSession;

/**
 * What a tool, resource or prompt handler is handed. It is the handler's channel back to
 * the client mid-request: progress updates, log lines, sampling, elicitation, roots.
 *
 * <p>
 * On streamable HTTP the underlying session is the <em>per-request stream</em>, so
 * anything emitted here is written onto the same SSE response the eventual result goes
 * to, exactly as the spec prescribes.
 */
public final class McpServerExchange {

  private final McpSession session;

  private final String sessionId;

  private final McpSchema.ClientCapabilities clientCapabilities;

  private final McpSchema.Implementation clientInfo;

  private final McpTransportContext transportContext;

  private final Object progressToken;

  public McpServerExchange(McpSession session, String sessionId, McpSchema.ClientCapabilities clientCapabilities,
      McpSchema.Implementation clientInfo, McpTransportContext transportContext, Object progressToken) {
    this.session = session;
    this.sessionId = sessionId;
    this.clientCapabilities = clientCapabilities;
    this.clientInfo = clientInfo;
    this.transportContext = transportContext;
    this.progressToken = progressToken;
  }

  public String sessionId() {
    return this.sessionId;
  }

  public McpSchema.ClientCapabilities clientCapabilities() {
    return this.clientCapabilities;
  }

  public McpSchema.Implementation clientInfo() {
    return this.clientInfo;
  }

  /** Transport metadata -- HTTP headers, principal, whatever the extractor put here. */
  public McpTransportContext transportContext() {
    return this.transportContext;
  }

  /** The client's progress token for this request, or {@code null} if it sent none. */
  public Object progressToken() {
    return this.progressToken;
  }

  public McpSession session() {
    return this.session;
  }

  // --- server -> client requests -------------------------------------------

  /** Asks the client's LLM to complete a message. Requires the sampling capability. */
  public Future<McpSchema.CreateMessageResult> createMessage(McpSchema.CreateMessageRequest request) {
    if (this.clientCapabilities == null || this.clientCapabilities.sampling() == null) {
      return Future.failedFuture(McpErrors.invalidRequest("Client does not support sampling"));
    }
    return this.session.sendRequest(McpSchema.METHOD_SAMPLING_CREATE_MESSAGE, request,
        new TypeRef<McpSchema.CreateMessageResult>() {
        });
  }

  /** Asks the user, through the client, for structured input. */
  public Future<McpSchema.ElicitResult> createElicitation(McpSchema.ElicitRequest request) {
    if (this.clientCapabilities == null || this.clientCapabilities.elicitation() == null) {
      return Future.failedFuture(McpErrors.invalidRequest("Client does not support elicitation"));
    }
    return this.session.sendRequest(McpSchema.METHOD_ELICITATION_CREATE, request,
        new TypeRef<McpSchema.ElicitResult>() {
        });
  }

  public Future<McpSchema.ListRootsResult> listRoots() {
    return listRoots(null);
  }

  public Future<McpSchema.ListRootsResult> listRoots(String cursor) {
    return this.session.sendRequest(McpSchema.METHOD_ROOTS_LIST, new McpSchema.PaginatedRequest(cursor, null),
        new TypeRef<McpSchema.ListRootsResult>() {
        });
  }

  public Future<Void> ping() {
    return this.session.sendRequest(McpSchema.METHOD_PING, null, new TypeRef<Object>() {
    }).mapEmpty();
  }

  // --- server -> client notifications --------------------------------------

  /**
   * Sends a log line. Silently dropped -- without a round trip -- if it is below the
   * level the client asked for via {@code logging/setLevel}.
   */
  public Future<Void> log(McpSchema.LoggingMessageNotification notification) {
    if (!this.session.isLoggingLevelEnabled(notification.level())) {
      return Future.succeededFuture();
    }
    return this.session.sendNotification(McpSchema.METHOD_NOTIFICATION_MESSAGE, notification);
  }

  public Future<Void> log(McpSchema.LoggingLevel level, String logger, String message) {
    return log(new McpSchema.LoggingMessageNotification(level, logger, message, null));
  }

  public Future<Void> progress(McpSchema.ProgressNotification notification) {
    return this.session.sendNotification(McpSchema.METHOD_NOTIFICATION_PROGRESS, notification);
  }

  /**
   * Reports progress against the token the client supplied. No-op when the client did not
   * ask for progress, so handlers can call it unconditionally.
   */
  public Future<Void> progress(double progress, Double total, String message) {
    if (this.progressToken == null) {
      return Future.succeededFuture();
    }
    return progress(new McpSchema.ProgressNotification(this.progressToken, progress, total, message, null));
  }
}
