package io.vertx.mcp.client.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.mcp.internal.Contexts;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpTransports.McpClientTransport;

/**
 * Runs an MCP server as a child process and talks newline-delimited JSON-RPC to it over
 * its standard streams -- the transport local, single-user servers use.
 *
 * <p>
 * {@link Process} only offers blocking streams, so the two readers and the writer live on
 * Vert.x worker threads and hand every decoded message back to the transport's own
 * context. Nothing blocking ever touches the event loop. The reader pool is
 * {@code ordered = false} because stdout and stderr must be drained concurrently -- a
 * child that fills its stderr pipe while we are parked on stdout would deadlock -- while
 * the writer pool is ordered so messages reach stdin in the order they were sent.
 *
 * <pre>{@code
 * var transport = StdioClientTransport.builder(vertx)
 *     .command("npx")
 *     .args("-y", "@modelcontextprotocol/server-filesystem", "/tmp")
 *     .stderrConsumer(line -> logger.info("[server] {}", line))
 *     .build();
 * }</pre>
 */
public final class StdioClientTransport implements McpClientTransport {

  private static final Logger logger = LoggerFactory.getLogger(StdioClientTransport.class);

  private static final AtomicInteger POOL_COUNTER = new AtomicInteger();

  private final Vertx vertx;

  private final Context context;

  private final List<String> command;

  private final Map<String, String> environment;

  private final boolean inheritEnvironment;

  private final Consumer<String> stderrConsumer;

  private final Duration terminationGrace;

  private final JsonRpcCodec codec;

  private final List<String> protocolVersions;

  private WorkerExecutor readers;

  private WorkerExecutor writer;

  private Process process;

  private BufferedWriter stdin;

  private Consumer<JSONRPCMessage> inbound;

  private Consumer<Throwable> exceptionHandler;

  private boolean closed;

  private StdioClientTransport(Builder builder) {
    this.vertx = builder.vertx;
    this.context = Contexts.current(builder.vertx);
    this.command = List.copyOf(builder.command);
    this.environment = Map.copyOf(builder.environment);
    this.inheritEnvironment = builder.inheritEnvironment;
    this.stderrConsumer = builder.stderrConsumer;
    this.terminationGrace = builder.terminationGrace;
    this.codec = builder.codec != null ? builder.codec : JsonRpcCodec.defaultCodec();
    this.protocolVersions = List.copyOf(builder.protocolVersions);
  }

  public static Builder builder(Vertx vertx) {
    return new Builder(vertx);
  }

  /** The child process, once {@link #connect} has started it. */
  public Process process() {
    return this.process;
  }

  @Override
  public List<String> protocolVersions() {
    return this.protocolVersions;
  }

  @Override
  public void exceptionHandler(Consumer<Throwable> handler) {
    this.exceptionHandler = handler;
  }

  // --- lifecycle -----------------------------------------------------------

  @Override
  public Future<Void> connect(Consumer<JSONRPCMessage> inboundHandler) {
    return Contexts.on(this.context, () -> {
      if (this.process != null) {
        return Future.failedFuture(new IllegalStateException("Transport is already connected"));
      }
      this.inbound = inboundHandler;
      int id = POOL_COUNTER.incrementAndGet();
      this.readers = this.vertx.createSharedWorkerExecutor("mcp-stdio-read-" + id, 2);
      this.writer = this.vertx.createSharedWorkerExecutor("mcp-stdio-write-" + id, 1);
      try {
        ProcessBuilder builder = new ProcessBuilder(this.command);
        if (!this.inheritEnvironment) {
          builder.environment().clear();
        }
        builder.environment().putAll(this.environment);
        this.process = builder.start();
      }
      catch (IOException e) {
        return Future.failedFuture(new McpTransportException("Could not start " + this.command, e));
      }
      this.stdin = new BufferedWriter(new OutputStreamWriter(this.process.getOutputStream(), StandardCharsets.UTF_8));
      this.readers.executeBlocking(this::pumpStdout, false).onFailure(this::onPumpFailure);
      this.readers.executeBlocking(this::pumpStderr, false).onFailure(this::onPumpFailure);
      return Future.succeededFuture();
    });
  }

  private Void pumpStdout() throws IOException {
    readLines(this.process.getInputStream(), line -> {
      JSONRPCMessage message;
      try {
        message = this.codec.decode(line);
      }
      catch (Exception e) {
        notifyException(new McpTransportException("Child process wrote a line that is not JSON-RPC: " + line, e));
        return;
      }
      Consumer<JSONRPCMessage> handler = this.inbound;
      if (handler != null) {
        Contexts.run(this.context, () -> handler.accept(message));
      }
    });
    if (!this.closed) {
      notifyException(new McpTransportException("Child process closed its stdout"));
    }
    return null;
  }

  private Void pumpStderr() throws IOException {
    readLines(this.process.getErrorStream(), line -> {
      Consumer<String> consumer = this.stderrConsumer;
      if (consumer != null) {
        Contexts.run(this.context, () -> consumer.accept(line));
      }
    });
    return null;
  }

  private void readLines(InputStream stream, Consumer<String> lineConsumer) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isBlank()) {
          lineConsumer.accept(line);
        }
      }
    }
    catch (IOException e) {
      // Closing our end during shutdown surfaces as an IOException on the blocked read;
      // that is the intended way out, not a failure.
      if (!this.closed) {
        throw e;
      }
    }
  }

  private void onPumpFailure(Throwable cause) {
    if (!this.closed) {
      notifyException(cause);
    }
  }

  // --- outbound ------------------------------------------------------------

  @Override
  public Future<Void> sendMessage(JSONRPCMessage message) {
    if (this.closed) {
      return Future.failedFuture(new IllegalStateException("Transport is closed"));
    }
    BufferedWriter out = this.stdin;
    if (out == null) {
      return Future.failedFuture(new IllegalStateException("Transport is not connected"));
    }
    String json = this.codec.encode(message);
    if (json.indexOf('\n') >= 0) {
      return Future.failedFuture(
          new McpTransportException("Encoded message contains a newline, which would corrupt the framing"));
    }
    return this.writer.executeBlocking(() -> {
      // Ordered pool of one: writes cannot interleave, so no lock is needed here.
      out.write(json);
      out.newLine();
      out.flush();
      return (Void) null;
    }, true);
  }

  @Override
  public Future<Void> close() {
    return Contexts.on(this.context, () -> {
      if (this.closed) {
        return Future.succeededFuture();
      }
      this.closed = true;
      Process child = this.process;
      BufferedWriter out = this.stdin;
      this.stdin = null;
      return this.vertx.executeBlocking(() -> {
        // Closing stdin is the polite shutdown signal for an MCP stdio server; the
        // destroy calls are the fallback for one that ignores it.
        closeQuietly(out);
        if (child != null) {
          child.destroy();
          try {
            if (!child.waitFor(this.terminationGrace.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
              child.destroyForcibly();
            }
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            child.destroyForcibly();
          }
        }
        return (Void) null;
      }).transform(result -> closeExecutors());
    });
  }

  private Future<Void> closeExecutors() {
    List<Future<?>> closings = new ArrayList<>();
    if (this.readers != null) {
      closings.add(this.readers.close());
    }
    if (this.writer != null) {
      closings.add(this.writer.close());
    }
    return Future.all(closings).mapEmpty();
  }

  private static void closeQuietly(BufferedWriter out) {
    if (out == null) {
      return;
    }
    try {
      out.close();
    }
    catch (IOException e) {
      logger.debug("Failed to close the child process stdin", e);
    }
  }

  private void notifyException(Throwable cause) {
    Consumer<Throwable> handler = this.exceptionHandler;
    if (handler != null) {
      Contexts.run(this.context, () -> handler.accept(cause));
    }
    else {
      logger.warn("Stdio transport failure for {}", this.command, cause);
    }
  }

  // --- builder -------------------------------------------------------------

  /** Fluent configuration, mirroring {@code VertxMcpServer.Builder}. */
  public static final class Builder {

    private final Vertx vertx;

    private final List<String> command = new ArrayList<>();

    private final Map<String, String> environment = new LinkedHashMap<>();

    private boolean inheritEnvironment = true;

    private Consumer<String> stderrConsumer = line -> logger.info("[mcp-server] {}", line);

    private Duration terminationGrace = Duration.ofSeconds(5);

    private JsonRpcCodec codec;

    private List<String> protocolVersions = new ArrayList<>(
        List.of(ProtocolVersions.MCP_2024_11_05, ProtocolVersions.MCP_2025_03_26, ProtocolVersions.MCP_2025_06_18));

    private Builder(Vertx vertx) {
      this.vertx = vertx;
    }

    /** The executable to run. Call {@link #args} for its arguments. */
    public Builder command(String command) {
      if (this.command.isEmpty()) {
        this.command.add(command);
      }
      else {
        this.command.set(0, command);
      }
      return this;
    }

    /** Sets the whole command line at once, executable first. */
    public Builder command(List<String> command) {
      this.command.clear();
      this.command.addAll(command);
      return this;
    }

    public Builder args(String... args) {
      this.command.addAll(List.of(args));
      return this;
    }

    public Builder args(List<String> args) {
      this.command.addAll(args);
      return this;
    }

    /** Environment entries for the child, added on top of the inherited environment. */
    public Builder env(String name, String value) {
      this.environment.put(name, value);
      return this;
    }

    public Builder env(Map<String, String> environment) {
      this.environment.putAll(environment);
      return this;
    }

    /**
     * Whether the child starts from this process's environment. Turn it off to hand the
     * child exactly the entries given to {@link #env}, and nothing else.
     */
    public Builder inheritEnvironment(boolean inheritEnvironment) {
      this.inheritEnvironment = inheritEnvironment;
      return this;
    }

    /** Where the child's stderr lines go. Defaults to this class's logger. */
    public Builder stderrConsumer(Consumer<String> stderrConsumer) {
      this.stderrConsumer = stderrConsumer;
      return this;
    }

    /** How long a closing child gets before it is killed. Default 5s. */
    public Builder terminationGrace(Duration terminationGrace) {
      this.terminationGrace = terminationGrace;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    /** Protocol versions to offer at initialize, newest last. */
    public Builder protocolVersions(List<String> protocolVersions) {
      this.protocolVersions = new ArrayList<>(protocolVersions);
      return this;
    }

    public StdioClientTransport build() {
      if (this.command.isEmpty()) {
        throw new IllegalArgumentException("A command is required");
      }
      return new StdioClientTransport(this);
    }
  }
}
