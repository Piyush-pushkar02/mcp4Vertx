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

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.vertx.core.buffer.Buffer;

/**
 * A {@link McpJsonMapper} implementation backed by Jackson 2 -- the same Jackson major
 * version Vert.x itself uses -- so an application ends up with exactly one Jackson on the
 * classpath.
 *
 * <p>
 * The upstream SDK ships {@code mcp-json-jackson3}, which drags in Jackson 3 alongside
 * whatever Jackson 2 Vert.x is already using. This module deliberately avoids that.
 */
public final class McpJson implements McpJsonMapper {

  private static final McpJson DEFAULT = new McpJson(defaultObjectMapper());

  private final ObjectMapper mapper;

  public McpJson(ObjectMapper mapper) {
    this.mapper = java.util.Objects.requireNonNull(mapper, "mapper must not be null");
  }

  /**
   * The shared default mapper. Safe to use from any thread; Jackson's {@code ObjectMapper}
   * is thread-safe once configured.
   */
  public static McpJson defaultMapper() {
    return DEFAULT;
  }

  /**
   * A mapper configured the way the MCP wire format needs: unknown properties are
   * tolerated (forward compatibility with newer protocol revisions) and {@code null}
   * fields are written, since some MCP results distinguish absent from null.
   */
  public static ObjectMapper defaultObjectMapper() {
    return JsonMapper.builder()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build();
  }

  public ObjectMapper objectMapper() {
    return this.mapper;
  }

  // --- McpJsonMapper -------------------------------------------------------

  @Override
  public <T> T readValue(String content, Class<T> type) throws IOException {
    return this.mapper.readValue(content, type);
  }

  @Override
  public <T> T readValue(byte[] content, Class<T> type) throws IOException {
    return this.mapper.readValue(content, type);
  }

  @Override
  public <T> T readValue(String content, TypeRef<T> type) throws IOException {
    return this.mapper.readValue(content, javaType(type));
  }

  @Override
  public <T> T readValue(byte[] content, TypeRef<T> type) throws IOException {
    return this.mapper.readValue(content, javaType(type));
  }

  @Override
  public <T> T convertValue(Object fromValue, Class<T> type) {
    return this.mapper.convertValue(fromValue, type);
  }

  @Override
  public <T> T convertValue(Object fromValue, TypeRef<T> type) {
    return this.mapper.convertValue(fromValue, javaType(type));
  }

  @Override
  public String writeValueAsString(Object value) throws IOException {
    return this.mapper.writeValueAsString(value);
  }

  @Override
  public byte[] writeValueAsBytes(Object value) throws IOException {
    return this.mapper.writeValueAsBytes(value);
  }

  // --- Vert.x conveniences -------------------------------------------------

  /** Serialize to a Vert.x {@link Buffer}, wrapping the checked exception. */
  public Buffer encodeToBuffer(Object value) {
    try {
      return Buffer.buffer(this.mapper.writeValueAsBytes(value));
    }
    catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize " + value.getClass().getName(), e);
    }
  }

  /** Serialize to a String, wrapping the checked exception. */
  public String encode(Object value) {
    try {
      return this.mapper.writeValueAsString(value);
    }
    catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize " + value.getClass().getName(), e);
    }
  }

  private <T> JavaType javaType(TypeRef<T> ref) {
    return this.mapper.getTypeFactory().constructType(ref.getType());
  }
}
