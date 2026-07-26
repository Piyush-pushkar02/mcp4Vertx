package io.vertx.mcp.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

/**
 * Uses shell one-liners as stand-in servers: {@code cat} echoes stdin back to stdout,
 * which is enough to prove the newline framing round-trips through a real process.
 */
@ExtendWith(VertxExtension.class)
@EnabledIf("hasShell")
class StdioClientTransportTest {

  static boolean hasShell() {
    return Files.isExecutable(Path.of("/bin/sh")) && Files.isExecutable(Path.of("/bin/cat"));
  }

  @Test
  void roundTripsAMessageThroughTheChildProcess(Vertx vertx, VertxTestContext testContext) throws Exception {
    List<JSONRPCMessage> received = new ArrayList<>();
    VertxTestContext echoed = new VertxTestContext();
    StdioClientTransport transport = StdioClientTransport.builder(vertx).command("/bin/cat").build();
    transport.connect(message -> {
      received.add(message);
      echoed.completeNow();
    })
      .compose(connected -> transport
        .sendMessage(new JSONRPCRequest(McpSchema.JSONRPC_VERSION, McpSchema.METHOD_PING, "c-1", null)))
      .onFailure(testContext::failNow);

    assertThat(echoed.awaitCompletion(15, TimeUnit.SECONDS)).isTrue();
    testContext.verify(() -> {
      assertThat(received).hasSize(1);
      assertThat(((JSONRPCRequest) received.get(0)).method()).isEqualTo(McpSchema.METHOD_PING);
      transport.close().onComplete(testContext.succeedingThenComplete());
    });
  }

  @Test
  void pipesStderrToTheConfiguredConsumer(Vertx vertx, VertxTestContext testContext) throws Exception {
    List<String> stderr = new ArrayList<>();
    VertxTestContext logged = new VertxTestContext();
    StdioClientTransport transport = StdioClientTransport.builder(vertx)
      .command("/bin/sh")
      .args("-c", "echo boom >&2; cat")
      .stderrConsumer(line -> {
        stderr.add(line);
        logged.completeNow();
      })
      .build();
    transport.connect(message -> {
    }).onFailure(testContext::failNow);

    assertThat(logged.awaitCompletion(15, TimeUnit.SECONDS)).isTrue();
    testContext.verify(() -> {
      assertThat(stderr).containsExactly("boom");
      transport.close().onComplete(testContext.succeedingThenComplete());
    });
  }

  @Test
  void reportsAFailureToStartTheChild(Vertx vertx, VertxTestContext testContext) {
    StdioClientTransport.builder(vertx)
      .command("/definitely/not/a/real/binary")
      .build()
      .connect(message -> {
      })
      .onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertThat(cause).isInstanceOf(McpTransportException.class);
        testContext.completeNow();
      })));
  }
}
