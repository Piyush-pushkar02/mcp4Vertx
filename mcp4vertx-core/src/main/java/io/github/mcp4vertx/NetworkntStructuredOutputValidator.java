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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import io.modelcontextprotocol.json.schema.JsonSchemaValidator;

/**
 * Structured-output validation on top of networknt, using the same Jackson 2
 * {@link ObjectMapper} as the rest of the stack.
 *
 * <p>
 * Compiled schemas are cached: tool output schemas are fixed at registration time, so
 * recompiling per call would be pure waste on a hot path.
 */
final class NetworkntStructuredOutputValidator implements McpStructuredOutputValidator {

  private final ObjectMapper mapper;

  private final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

  private final Map<Map<String, Object>, JsonSchema> cache = new ConcurrentHashMap<>();

  NetworkntStructuredOutputValidator(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public JsonSchemaValidator.ValidationResponse validate(Map<String, Object> schema, Object structuredContent) {
    if (schema == null || schema.isEmpty()) {
      return JsonSchemaValidator.ValidationResponse.asValid(null);
    }
    if (structuredContent == null) {
      return JsonSchemaValidator.ValidationResponse
        .asInvalid("Tool declares an outputSchema but returned no structuredContent");
    }
    try {
      JsonSchema compiled = this.cache.computeIfAbsent(schema,
          s -> this.factory.getSchema(this.mapper.valueToTree(s)));
      JsonNode node = this.mapper.valueToTree(structuredContent);
      Set<ValidationMessage> errors = compiled.validate(node);
      if (!errors.isEmpty()) {
        return JsonSchemaValidator.ValidationResponse.asInvalid(
            errors.stream().map(ValidationMessage::getMessage).collect(Collectors.joining("; ")));
      }
      return JsonSchemaValidator.ValidationResponse.asValid(this.mapper.writeValueAsString(node));
    }
    catch (Exception e) {
      return JsonSchemaValidator.ValidationResponse.asInvalid("Schema validation failed: " + e.getMessage());
    }
  }
}
