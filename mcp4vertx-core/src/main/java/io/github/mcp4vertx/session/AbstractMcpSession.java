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
package io.github.mcp4vertx.session;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.internal.Contexts;
import io.github.mcp4vertx.spec.JsonRpcCodec;
import io.github.mcp4vertx.spec.McpErrors;
import io.github.mcp4vertx.spec.McpHandlers;
import io.github.mcp4vertx.spec.McpMethods;
import io.github.mcp4vertx.spec.McpRequestContext;
import io.github.mcp4vertx.spec.McpSession;

/**
 * Shared machinery for both ends of a JSON-RPC conversation: id generation, request/
 * response correlation, timeouts, cancellation and handler dispatch.
 *
 * <p>
 * <b>Threading.</b> The session is pinned to the Vert.x {@link Context} it was constructed
 * on. Every mutation of {@link #pendingRequests} and {@link #cancelledRequests} happens on
 * that context, so both are plain {@link HashMap}/{@link HashSet} -- no locks, no
 * concurrent collections, and strictly ordered message delivery per session. Calls made
 * from other threads hop via {@link Contexts#on}.
 */
public abstract class AbstractMcpSession implements McpSession {

  private static final Logger logger = LoggerFactory.getLogger(AbstractMcpSession.class);

  protected final Vertx vertx;

  protected final Context context;

  protected final JsonRpcCodec codec;

  protected final Duration requestTimeout;

  protected final Map<String, McpHandlers.RequestHandler> requestHandlers;

  protected final Map<String, McpHandlers.NotificationHandler> notificationHandlers;

  private final AtomicLong requestCounter = new AtomicLong();

  private final Map<Object, Pending> pendingRequests = new HashMap<>();

  private final Set<Object> cancelledRequests = new HashSet<>();

  private volatile McpSchema.LoggingLevel minLoggingLevel = McpSchema.LoggingLevel.INFO;

  private boolean closed;

  protected AbstractMcpSession(Vertx vertx, JsonRpcCodec codec, Duration requestTimeout,
      Map<String, McpHandlers.RequestHandler> requestHandlers,
      Map<String, McpHandlers.NotificationHandler> notificationHandlers) {
    this.vertx = vertx;
    this.context = Contexts.current(vertx);
    this.codec = codec;
    this.requestTimeout = requestTimeout;
    this.requestHandlers = requestHandlers;
    this.notificationHandlers = notificationHandlers;
  }

  /** Writes a message out on whatever this session's default outbound stream is. */
  protected abstract Future<Void> write(JSONRPCMessage message);

  /** Prefix for generated request ids; must be unique per session. */
  protected abstract String idPrefix();

  /** The session this handler-invocation should reply through. Overridden per-stream. */
  protected McpSession replySession() {
    return this;
  }

  protected String sessionId() {
    return null;
  }

  public Context vertxContext() {
    return this.context;
  }

  public JsonRpcCodec codec() {
    return this.codec;
  }

  // --- outbound ------------------------------------------------------------

  @Override
  public <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType) {
    return sendRequest(method, params, resultType, this::write, id -> {
    });
  }

  /**
   * Sends a request over a caller-chosen writer.
   *
   * <p>
   * Correlation and timeout bookkeeping always live on the session, never on the
   * individual stream, because the client answers a server-initiated request with a fresh
   * HTTP POST -- which lands on the session, not on the stream that asked.
   *
   * @param writer writes the request bytes (a specific SSE stream, say)
   * @param onIdAssigned notified with the generated id so the caller can record which
   * stream owns it
   */
  protected <T> Future<T> sendRequest(String method, Object params, TypeRef<T> resultType,
      java.util.function.Function<JSONRPCRequest, Future<Void>> writer,
      java.util.function.Consumer<String> onIdAssigned) {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.failedFuture(new IllegalStateException("Session is closed"));
      }
      String id = idPrefix() + "-" + this.requestCounter.incrementAndGet();
      Promise<T> promise = Promise.promise();
      long timerId = this.vertx.setTimer(this.requestTimeout.toMillis(), t -> {
        Pending removed = this.pendingRequests.remove(id);
        if (removed != null) {
          removed.promise.tryFail(new TimeoutException(
              "No response to '" + method + "' (id=" + id + ") within " + this.requestTimeout));
        }
      });
      this.pendingRequests.put(id, new Pending(promise, resultType, timerId));
      onIdAssigned.accept(id);

      JSONRPCRequest request = new JSONRPCRequest(McpSchema.JSONRPC_VERSION, method, id, params);
      writer.apply(request).onFailure(err -> failPending(id, err));
      @SuppressWarnings("unchecked")
      Future<T> future = (Future<T>) promise.future();
      return future;
    });
  }

  /** Fails and forgets one in-flight request. No-op if it already completed. */
  protected void failPending(Object id, Throwable cause) {
    Pending removed = this.pendingRequests.remove(id);
    if (removed != null) {
      this.vertx.cancelTimer(removed.timerId);
      removed.promise.tryFail(cause);
    }
  }

  @Override
  public Future<Void> sendNotification(String method, Object params) {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.failedFuture(new IllegalStateException("Session is closed"));
      }
      return write(new JSONRPCNotification(McpSchema.JSONRPC_VERSION, method, params));
    });
  }

  // --- inbound -------------------------------------------------------------

  /**
   * Completes the matching pending request. Runs on the session context.
   */
  protected void handleResponse(JSONRPCResponse response) {
    Pending pending = this.pendingRequests.remove(response.id());
    if (pending == null) {
      logger.debug("Dropping response for unknown request id {}", response.id());
      return;
    }
    this.vertx.cancelTimer(pending.timerId);
    if (response.error() != null) {
      pending.promise.tryFail(new McpError(response.error()));
      return;
    }
    try {
      Object result = response.result() == null ? null
          : this.codec.unmarshal(response.result(), pending.resultType);
      pending.promise.tryComplete(result);
    }
    catch (Exception e) {
      pending.promise.tryFail(e);
    }
  }

  /**
   * Runs the registered handler for a request and always yields a well-formed response --
   * handler failures become JSON-RPC errors rather than propagating.
   *
   * @return {@code null} inside the future if the request was cancelled while in flight,
   * in which case nothing should be written back
   */
  protected Future<JSONRPCResponse> invokeRequestHandler(JSONRPCRequest request, McpTransportContext transportContext,
      McpSession replyTo) {
    McpHandlers.RequestHandler handler = this.requestHandlers.get(request.method());
    if (handler == null) {
      return Future.succeededFuture(McpErrors.errorResponse(request.id(), McpErrors.methodNotFound(request.method())));
    }
    McpRequestContext ctx = McpRequestContext.of(replyTo, sessionId(), transportContext, request.params(),
        request.id());
    Future<Object> result;
    try {
      result = handler.handle(ctx);
    }
    catch (Throwable t) {
      result = Future.failedFuture(t);
    }
    return result
      .map(value -> new JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), value, null))
      .otherwise(err -> McpErrors.errorResponse(request.id(), err))
      .map(response -> this.cancelledRequests.remove(request.id()) ? null : response);
  }

  protected Future<Void> invokeNotificationHandler(JSONRPCNotification notification,
      McpTransportContext transportContext) {
    if (McpMethods.NOTIFICATION_CANCELLED.equals(notification.method())) {
      markCancelled(notification.params());
      return Future.succeededFuture();
    }
    McpHandlers.NotificationHandler handler = this.notificationHandlers.get(notification.method());
    if (handler == null) {
      logger.debug("No handler for notification {}", notification.method());
      return Future.succeededFuture();
    }
    McpRequestContext ctx = McpRequestContext.of(replySession(), sessionId(), transportContext,
        notification.params(), null);
    try {
      return handler.handle(ctx).recover(err -> {
        logger.warn("Notification handler for {} failed", notification.method(), err);
        return Future.succeededFuture();
      });
    }
    catch (Throwable t) {
      logger.warn("Notification handler for {} threw", notification.method(), t);
      return Future.succeededFuture();
    }
  }

  private void markCancelled(Object params) {
    if (params instanceof Map<?, ?> map) {
      Object requestId = map.get("requestId");
      if (requestId != null) {
        this.cancelledRequests.add(requestId);
        logger.debug("Client cancelled request {}", requestId);
      }
    }
  }

  // --- logging level -------------------------------------------------------

  @Override
  public boolean isLoggingLevelEnabled(McpSchema.LoggingLevel level) {
    return level.level() >= this.minLoggingLevel.level();
  }

  @Override
  public void setMinLoggingLevel(McpSchema.LoggingLevel level) {
    this.minLoggingLevel = level;
  }

  // --- lifecycle -----------------------------------------------------------

  @Override
  public Future<Void> close() {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      this.pendingRequests.forEach((id, pending) -> {
        this.vertx.cancelTimer(pending.timerId);
        pending.promise.tryFail(new IllegalStateException("Session closed before response to request " + id));
      });
      this.pendingRequests.clear();
      this.cancelledRequests.clear();
      return doClose();
    });
  }

  protected Future<Void> doClose() {
    return Future.succeededFuture();
  }

  protected boolean isClosed() {
    return this.closed;
  }

  @SuppressWarnings("rawtypes")
  private record Pending(Promise promise, TypeRef resultType, long timerId) {
  }
}
