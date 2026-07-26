package io.vertx.mcp.spec;

import io.vertx.mcp.McpTransportContext;

/**
 * Everything a request handler needs, passed explicitly rather than through a
 * thread-local or a subscriber context.
 *
 * @param session the session to send server-initiated messages on. For streamable HTTP
 * this is the per-request stream, so notifications a handler emits ride back on the same
 * SSE response as the result -- which is what the spec asks for.
 * @param sessionId the id of the owning session, or {@code null} when stateless
 * @param transportContext transport metadata (headers, principal, ...)
 * @param params the raw, still-unmarshalled JSON-RPC params
 * @param requestId the JSON-RPC id of the request being handled
 * @param progressToken the client's progress token if it supplied one, else {@code null}
 */
public record McpRequestContext(McpSession session, String sessionId, McpTransportContext transportContext,
    Object params, Object requestId, Object progressToken) {

  public static McpRequestContext of(McpSession session, String sessionId, McpTransportContext transportContext,
      Object params, Object requestId) {
    return new McpRequestContext(session, sessionId, transportContext, params, requestId,
        extractProgressToken(params));
  }

  @SuppressWarnings("unchecked")
  private static Object extractProgressToken(Object params) {
    if (params instanceof java.util.Map<?, ?> map) {
      Object meta = map.get("_meta");
      if (meta instanceof java.util.Map<?, ?> metaMap) {
        return ((java.util.Map<String, Object>) metaMap).get("progressToken");
      }
    }
    return null;
  }
}
