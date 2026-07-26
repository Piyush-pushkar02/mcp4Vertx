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
 * Implemented by transports that have to put the negotiated protocol version on the wire.
 *
 * <p>
 * The HTTP transports must send {@code MCP-Protocol-Version} on every request after the
 * handshake, but the negotiation happens a layer above them, in
 * {@link io.github.mcp4vertx.client.VertxMcpClient}. Rather than teach the core transport SPI
 * about it, the client checks for this interface and pushes the result down.
 */
public interface McpProtocolVersionAware {

  /** Called once, with the version the server chose in its {@code initialize} response. */
  void negotiatedProtocolVersion(String protocolVersion);
}
