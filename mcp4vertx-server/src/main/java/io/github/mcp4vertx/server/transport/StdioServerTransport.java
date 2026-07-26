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
package io.github.mcp4vertx.server.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.github.mcp4vertx.McpTransportContext;
import io.github.mcp4vertx.internal.Contexts;
import io.github.mcp4vertx.server.VertxMcpServer;
import io.github.mcp4vertx.session.McpServerSession;
import io.github.mcp4vertx.spec.JsonRpcCodec;
import io.github.mcp4vertx.spec.McpTransports.McpServerTransport;

/**
 * The stdio server transport: newline-delimited JSON-RPC on {@code System.in} and
 * {@code System.out}, which is how a desktop MCP host launches and talks to a local
 * server process.
 *
 * <p>
 * <b>stdout is the protocol channel.</b> A single stray {@code println} corrupts the
 * stream and the host drops the connection, so this class never routes anything through
 * SLF4J -- whose default console appender writes to stdout -- and logs to
 * {@code System.err} directly instead. Configure your own logging the same way when you
 * deploy over stdio.
 *
 * <p>
 * {@code System.in} has no non-blocking API, so the read loop runs on a Vert.x worker
 * thread and hands each parsed message back to the session's event-loop context. It parks
 * that one worker thread for the life of the process, which is acceptable precisely
 * because stdio serves exactly one client and exists for exactly as long as the process
 * does.
 *
 * <pre>{@code
 * StdioServerTransport.create(vertx, server).start();
 * }</pre>
 */
public final class StdioServerTransport {

  /** Session id used for the single stdio conversation. */
  public static final String SESSION_ID = "stdio";

  private final Vertx vertx;

  private final JsonRpcCodec codec;

  private final InputStream in;

  private final PrintStream out;

  private final Context context;

  private final McpServerSession session;

  private final StdoutTransport transport;

  private volatile boolean running;

  private StdioServerTransport(Vertx vertx, VertxMcpServer server, InputStream in, OutputStream out) {
    this.vertx = vertx;
    this.codec = server.codec();
    this.in = in;
    this.out = out instanceof PrintStream printStream ? printStream
        : new PrintStream(out, false, StandardCharsets.UTF_8);
    this.context = Contexts.current(vertx);
    this.transport = new StdoutTransport();
    this.session = server.sessionFactory().create(SESSION_ID, this.transport);
  }

  /** Binds to the process's own {@code System.in} / {@code System.out}. */
  public static StdioServerTransport create(Vertx vertx, VertxMcpServer server) {
    return new StdioServerTransport(vertx, server, System.in, System.out);
  }

  /** Binds to arbitrary streams. Useful for tests and for embedding. */
  public static StdioServerTransport create(Vertx vertx, VertxMcpServer server, InputStream in, OutputStream out) {
    return new StdioServerTransport(vertx, server, in, out);
  }

  public McpServerSession session() {
    return this.session;
  }

  /**
   * Starts the read loop. The returned future completes when the input stream reaches EOF
   * -- i.e. when the host closed the pipe -- or fails if reading broke.
   */
  public Future<Void> start() {
    this.running = true;
    Promise<Void> finished = Promise.promise();
    // ordered=false: this task never returns, so it must not sit on the context's ordered
    // queue and starve the outbound writes below.
    this.vertx.executeBlocking(() -> {
      readLoop();
      return null;
    }, false).onComplete(ar -> {
      this.running = false;
      if (ar.failed()) {
        log("stdio read loop failed", ar.cause());
        finished.tryFail(ar.cause());
      }
      else {
        finished.tryComplete();
      }
    });
    return finished.future();
  }

  private void readLoop() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(this.in, StandardCharsets.UTF_8));
    String line;
    while (this.running && (line = reader.readLine()) != null) {
      if (line.isBlank()) {
        continue;
      }
      JSONRPCMessage message;
      try {
        message = this.codec.decode(line);
      }
      catch (Exception e) {
        // A malformed line has no id to answer against, so there is nothing well-formed
        // to write back. Drop it and keep the stream alive.
        log("Dropping unparseable stdio line: " + e.getMessage(), null);
        continue;
      }
      JSONRPCMessage dispatched = message;
      Contexts.run(this.context, () -> this.session.handle(dispatched, McpTransportContext.EMPTY)
        .onFailure(err -> log("stdio message handling failed", err)));
    }
  }

  /** Stops the read loop and closes the session. */
  public Future<Void> close() {
    this.running = false;
    return this.session.close();
  }

  private static void log(String message, Throwable cause) {
    System.err.println("[vertx-mcp stdio] " + message);
    if (cause != null) {
      cause.printStackTrace(System.err);
    }
  }

  /**
   * Writes one JSON message per line to stdout. Writes go through
   * {@code executeBlocking} in ordered mode, so a slow or full pipe never parks the event
   * loop and frames still leave in the order the session produced them.
   */
  private final class StdoutTransport implements McpServerTransport {

    private volatile boolean closed;

    @Override
    public Future<Void> sendMessage(JSONRPCMessage message) {
      if (this.closed) {
        return Future.failedFuture(new IllegalStateException("stdio transport is closed"));
      }
      String json = StdioServerTransport.this.codec.encode(message);
      return StdioServerTransport.this.vertx.executeBlocking(() -> {
        synchronized (StdioServerTransport.this.out) {
          StdioServerTransport.this.out.print(json);
          StdioServerTransport.this.out.print('\n');
          StdioServerTransport.this.out.flush();
        }
        return null;
      });
    }

    @Override
    public Future<Void> close() {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      StdioServerTransport.this.running = false;
      // stdout belongs to the process, not to us: flush it, but never close it.
      StdioServerTransport.this.out.flush();
      return Future.succeededFuture();
    }
  }
}
