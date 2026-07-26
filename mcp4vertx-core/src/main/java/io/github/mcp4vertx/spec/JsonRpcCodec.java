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

import java.io.IOException;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.buffer.Buffer;
import io.github.mcp4vertx.McpJson;

/** Turns JSON-RPC messages into bytes and back. */
public final class JsonRpcCodec {

  private final McpJsonMapper mapper;

  public JsonRpcCodec(McpJsonMapper mapper) {
    this.mapper = mapper;
  }

  public static JsonRpcCodec defaultCodec() {
    return new JsonRpcCodec(McpJson.defaultMapper());
  }

  public McpJsonMapper mapper() {
    return this.mapper;
  }

  public JSONRPCMessage decode(String json) throws IOException {
    return McpSchema.deserializeJsonRpcMessage(this.mapper, json);
  }

  public JSONRPCMessage decode(Buffer buffer) throws IOException {
    return decode(buffer.toString(java.nio.charset.StandardCharsets.UTF_8));
  }

  public String encode(Object message) {
    try {
      return this.mapper.writeValueAsString(message);
    }
    catch (IOException e) {
      throw new IllegalStateException("Failed to serialize JSON-RPC message", e);
    }
  }

  public Buffer encodeToBuffer(Object message) {
    try {
      return Buffer.buffer(this.mapper.writeValueAsBytes(message));
    }
    catch (IOException e) {
      throw new IllegalStateException("Failed to serialize JSON-RPC message", e);
    }
  }

  /** Unmarshals loosely-typed JSON-RPC {@code params} into a concrete schema record. */
  public <T> T unmarshal(Object raw, Class<T> type) {
    return this.mapper.convertValue(raw, type);
  }

  public <T> T unmarshal(Object raw, io.modelcontextprotocol.json.TypeRef<T> type) {
    return this.mapper.convertValue(raw, type);
  }
}
