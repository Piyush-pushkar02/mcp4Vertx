package io.vertx.mcp.spec;

import java.io.IOException;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.mcp.McpJson;

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
