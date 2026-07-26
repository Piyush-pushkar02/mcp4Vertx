package io.vertx.mcp.spec;

import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse.JSONRPCError;

/** Helpers for turning arbitrary throwables into well-formed JSON-RPC errors. */
public final class McpErrors {

  private McpErrors() {
  }

  public static McpError methodNotFound(String method) {
    return new McpError(new JSONRPCError(McpSchema.ErrorCodes.METHOD_NOT_FOUND, "Method not found: " + method, null));
  }

  public static McpError invalidParams(String message) {
    return new McpError(new JSONRPCError(McpSchema.ErrorCodes.INVALID_PARAMS, message, null));
  }

  public static McpError invalidRequest(String message) {
    return new McpError(new JSONRPCError(McpSchema.ErrorCodes.INVALID_REQUEST, message, null));
  }

  public static McpError internalError(String message) {
    return new McpError(new JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR, message, null));
  }

  public static McpError resourceNotFound(String uri) {
    return McpError.RESOURCE_NOT_FOUND.apply(uri);
  }

  /**
   * Maps a throwable onto a JSON-RPC error object. {@link McpError} carries its own code
   * through untouched; anything else becomes {@code INTERNAL_ERROR} with the exception
   * chain flattened into {@code data} so the client gets something debuggable.
   */
  public static JSONRPCError toJsonRpcError(Throwable t) {
    if (t instanceof McpError mcpError && mcpError.getJsonRpcError() != null) {
      return mcpError.getJsonRpcError();
    }
    String message = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    return new JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR, message, McpError.aggregateExceptionMessages(t));
  }

  public static McpSchema.JSONRPCResponse errorResponse(Object requestId, Throwable t) {
    return new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, requestId, null, toJsonRpcError(t));
  }

  public static McpSchema.JSONRPCResponse errorResponse(Object requestId, int code, String message) {
    return new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, requestId, null,
        new JSONRPCError(code, message, null));
  }
}
