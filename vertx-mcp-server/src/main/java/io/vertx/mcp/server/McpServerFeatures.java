package io.vertx.mcp.server;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Future;

/** The units a server is built from. Each pairs a declaration with its handler. */
public final class McpServerFeatures {

  private McpServerFeatures() {
  }

  @FunctionalInterface
  public interface ToolHandler {
    Future<McpSchema.CallToolResult> handle(McpServerExchange exchange, McpSchema.CallToolRequest request);
  }

  /** Blocking variant, dispatched off the event loop. See {@code VertxMcpServer.toolBlocking}. */
  @FunctionalInterface
  public interface BlockingToolHandler {
    McpSchema.CallToolResult handle(McpServerExchange exchange, McpSchema.CallToolRequest request) throws Exception;
  }

  @FunctionalInterface
  public interface ResourceHandler {
    Future<McpSchema.ReadResourceResult> handle(McpServerExchange exchange, McpSchema.ReadResourceRequest request);
  }

  @FunctionalInterface
  public interface BlockingResourceHandler {
    McpSchema.ReadResourceResult handle(McpServerExchange exchange, McpSchema.ReadResourceRequest request)
        throws Exception;
  }

  @FunctionalInterface
  public interface PromptHandler {
    Future<McpSchema.GetPromptResult> handle(McpServerExchange exchange, McpSchema.GetPromptRequest request);
  }

  @FunctionalInterface
  public interface BlockingPromptHandler {
    McpSchema.GetPromptResult handle(McpServerExchange exchange, McpSchema.GetPromptRequest request) throws Exception;
  }

  @FunctionalInterface
  public interface CompletionHandler {
    Future<McpSchema.CompleteResult> handle(McpServerExchange exchange, McpSchema.CompleteRequest request);
  }

  public record ToolSpec(McpSchema.Tool tool, ToolHandler handler) {
  }

  public record ResourceSpec(McpSchema.Resource resource, ResourceHandler handler) {
  }

  /**
   * A templated resource. {@code uriTemplate} is RFC 6570; the handler receives the
   * concrete uri the client asked for.
   */
  public record ResourceTemplateSpec(McpSchema.ResourceTemplate template, ResourceHandler handler) {
  }

  public record PromptSpec(McpSchema.Prompt prompt, PromptHandler handler) {
  }

  public record CompletionSpec(McpSchema.CompleteReference reference, CompletionHandler handler) {
  }
}
