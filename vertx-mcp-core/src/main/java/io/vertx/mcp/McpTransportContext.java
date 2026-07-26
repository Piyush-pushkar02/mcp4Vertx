package io.vertx.mcp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Transport-level metadata carried alongside a request -- HTTP headers, an authenticated
 * principal, a tenant id, whatever the transport chose to extract.
 *
 * <p>
 * Unlike the upstream SDK, this is not smuggled through a Reactor subscriber context. It
 * travels as an explicit field on {@link io.vertx.mcp.spec.McpRequestContext} and is also
 * stashed in the Vert.x {@link io.vertx.core.Context} local data under {@link #CONTEXT_KEY},
 * so instrumentation that already reads Vert.x context locals can see it without any
 * bridging.
 */
public final class McpTransportContext {

  public static final String CONTEXT_KEY = "io.vertx.mcp.transportContext";

  public static final McpTransportContext EMPTY = new McpTransportContext(Map.of());

  private final Map<String, Object> values;

  private McpTransportContext(Map<String, Object> values) {
    this.values = values;
  }

  public static McpTransportContext of(Map<String, Object> values) {
    if (values == null || values.isEmpty()) {
      return EMPTY;
    }
    return new McpTransportContext(Collections.unmodifiableMap(new HashMap<>(values)));
  }

  public Object get(String key) {
    return this.values.get(key);
  }

  public <T> Optional<T> get(String key, Class<T> type) {
    Object value = this.values.get(key);
    return type.isInstance(value) ? Optional.of(type.cast(value)) : Optional.empty();
  }

  public Map<String, Object> asMap() {
    return this.values;
  }

  public boolean isEmpty() {
    return this.values.isEmpty();
  }

  /** Returns a copy with one additional entry. */
  public McpTransportContext with(String key, Object value) {
    Map<String, Object> copy = new HashMap<>(this.values);
    copy.put(key, value);
    return new McpTransportContext(Collections.unmodifiableMap(copy));
  }

  @Override
  public String toString() {
    return "McpTransportContext" + this.values.keySet();
  }
}
