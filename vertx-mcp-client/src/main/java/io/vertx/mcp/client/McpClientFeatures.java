package io.vertx.mcp.client;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;

/**
 * The handler contracts a client registers for traffic the server initiates.
 *
 * <p>
 * The request-shaped ones return {@link Future} because answering may involve a model
 * call or a user prompt; the notification-shaped ones return nothing because there is
 * nothing to write back. Grouped in one file for the same reason
 * {@link io.vertx.mcp.spec.McpHandlers} is: they are tiny and only meaningful together.
 */
public final class McpClientFeatures {

  private McpClientFeatures() {
  }

  /**
   * Answers {@code sampling/createMessage}: the server is asking this client to run an
   * LLM completion on its behalf, so that the server never needs model credentials.
   */
  @FunctionalInterface
  public interface SamplingHandler {
    Future<McpSchema.CreateMessageResult> handle(McpSchema.CreateMessageRequest request);
  }

  /**
   * Answers {@code elicitation/create}: the server needs structured input from the user.
   * Decline with {@link McpSchema.ElicitResult.Action#DECLINE} rather than failing the
   * future -- a refusal is a valid answer, not an error.
   */
  @FunctionalInterface
  public interface ElicitationHandler {
    Future<McpSchema.ElicitResult> handle(McpSchema.ElicitRequest request);
  }

  /** Receives {@code notifications/message}, the server's structured log stream. */
  @FunctionalInterface
  public interface LoggingHandler {
    void handle(McpSchema.LoggingMessageNotification notification);
  }

  /** Receives {@code notifications/progress} for a long-running request. */
  @FunctionalInterface
  public interface ProgressHandler {
    void handle(McpSchema.ProgressNotification notification);
  }

  /**
   * Receives a {@code list_changed} notification for tools, resources or prompts. The
   * notification carries no payload by design: re-list to see what changed.
   */
  @FunctionalInterface
  public interface ListChangedHandler {
    void handle();
  }

  /** Receives {@code notifications/resources/updated} for a subscribed resource. */
  @FunctionalInterface
  public interface ResourceUpdatedHandler {
    void handle(String uri);
  }
}
