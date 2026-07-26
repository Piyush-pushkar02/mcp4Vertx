package io.vertx.mcp;

import java.util.Map;

import io.modelcontextprotocol.json.schema.JsonSchemaValidator;

/**
 * Validates a tool's {@code structuredContent} against its declared {@code outputSchema}.
 *
 * <p>
 * Backed by {@code com.networknt:json-schema-validator} when that jar is present -- it is
 * an <em>optional</em> dependency here, so a deployment that does not care about
 * structured-output validation does not carry it. {@link #available()} reports which mode
 * you are in; when unavailable, {@link #permissive()} accepts everything.
 */
public interface McpStructuredOutputValidator {

  JsonSchemaValidator.ValidationResponse validate(Map<String, Object> schema, Object structuredContent);

  static boolean available() {
    return Holder.AVAILABLE;
  }

  /** The networknt-backed validator if on the classpath, otherwise {@link #permissive()}. */
  static McpStructuredOutputValidator defaultValidator() {
    return Holder.INSTANCE;
  }

  static McpStructuredOutputValidator permissive() {
    return (schema, structuredContent) -> JsonSchemaValidator.ValidationResponse.asValid(null);
  }

  final class Holder {

    static final boolean AVAILABLE;

    static final McpStructuredOutputValidator INSTANCE;

    static {
      boolean available;
      McpStructuredOutputValidator instance;
      try {
        Class.forName("com.networknt.schema.JsonSchemaFactory");
        instance = new NetworkntStructuredOutputValidator(McpJson.defaultMapper().objectMapper());
        available = true;
      }
      catch (Throwable t) {
        instance = permissive();
        available = false;
      }
      AVAILABLE = available;
      INSTANCE = instance;
    }

    private Holder() {
    }
  }
}
