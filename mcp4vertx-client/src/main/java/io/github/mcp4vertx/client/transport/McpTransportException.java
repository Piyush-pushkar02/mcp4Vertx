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
package io.github.mcp4vertx.client.transport;

/**
 * A transport-level failure that is not a protocol error: an unexpected HTTP status, a
 * body we could not decode, a child process that died. JSON-RPC errors returned by the
 * server arrive as {@link io.modelcontextprotocol.spec.McpError} on the request future
 * instead and never surface as this type.
 */
public class McpTransportException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public McpTransportException(String message) {
    super(message);
  }

  public McpTransportException(String message, Throwable cause) {
    super(message, cause);
  }
}
