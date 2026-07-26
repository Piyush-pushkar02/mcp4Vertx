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
package io.github.mcp4vertx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Transport-level metadata carried alongside a request -- HTTP headers, an authenticated
 * principal, a tenant id, whatever the transport chose to extract.
 *
 * <p>
 * Unlike the upstream SDK, this is not smuggled through a Reactor subscriber context: it
 * travels as an explicit field on {@link io.github.mcp4vertx.spec.McpRequestContext}, and reaches
 * handlers as {@code McpServerExchange.transportContext()}. Explicit beats ambient here --
 * a request-scoped value has no business in Vert.x context-local storage, which is shared by
 * every request on that event loop.
 */
public final class McpTransportContext {

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
