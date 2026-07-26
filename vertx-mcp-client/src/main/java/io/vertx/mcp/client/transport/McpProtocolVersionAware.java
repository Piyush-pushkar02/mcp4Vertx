package io.vertx.mcp.client.transport;

/**
 * Implemented by transports that have to put the negotiated protocol version on the wire.
 *
 * <p>
 * The HTTP transports must send {@code MCP-Protocol-Version} on every request after the
 * handshake, but the negotiation happens a layer above them, in
 * {@link io.vertx.mcp.client.VertxMcpClient}. Rather than teach the core transport SPI
 * about it, the client checks for this interface and pushes the result down.
 */
public interface McpProtocolVersionAware {

  /** Called once, with the version the server chose in its {@code initialize} response. */
  void negotiatedProtocolVersion(String protocolVersion);
}
