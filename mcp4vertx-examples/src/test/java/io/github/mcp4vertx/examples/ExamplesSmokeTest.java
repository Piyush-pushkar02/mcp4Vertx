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
package io.github.mcp4vertx.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.github.mcp4vertx.client.VertxMcpClient;
import io.github.mcp4vertx.client.transport.StreamableHttpClientTransport;

/**
 * The property this class establishes: the two files in {@code src/main} are working
 * documentation rather than pseudocode. {@link WeatherServerVerticle} deploys, serves its
 * non-MCP {@code /health} route and its MCP sub-router off the same port, and every
 * feature it advertises -- the async tool with progress, the blocking tool, the static
 * resource, the templated resource and the prompt -- answers correctly; and
 * {@link ClientExample} drives it to completion.
 */
@ExtendWith(VertxExtension.class)
class ExamplesSmokeTest {

  private WeatherServerVerticle verticle;

  private String baseUrl;

  @BeforeEach
  void deploy(Vertx vertx, VertxTestContext testContext) {
    this.verticle = new WeatherServerVerticle(0);
    vertx.deployVerticle(this.verticle).onComplete(testContext.succeeding(id -> {
      this.baseUrl = "http://localhost:" + this.verticle.port();
      testContext.completeNow();
    }));
  }

  @Test
  void theAppRouterServesHealthAlongsideTheMcpEndpoint(Vertx vertx, VertxTestContext testContext) {
    HttpClient http = vertx.createHttpClient();
    http
      .request(new RequestOptions().setMethod(HttpMethod.GET)
        .setPort(this.verticle.port())
        .setHost("localhost")
        .setURI("/health"))
      .compose(request -> request.send())
      .compose(response -> response.body().map(body -> Map.entry(response.statusCode(), body.toString())))
      .onComplete(testContext.succeeding(entry -> testContext.verify(() -> {
        assertThat(entry.getKey()).isEqualTo(200);
        assertThat(entry.getValue()).contains("\"status\":\"UP\"");
        testContext.completeNow();
      })));
  }

  @Test
  void everyFeatureTheWeatherServerAdvertisesAnswers(Vertx vertx, VertxTestContext testContext) {
    List<McpSchema.ProgressNotification> progress = new CopyOnWriteArrayList<>();
    VertxMcpClient client = VertxMcpClient.builder(vertx)
      .transport(StreamableHttpClientTransport.builder(vertx).baseUrl(this.baseUrl).endpoint("/mcp").build())
      .clientInfo("smoke", "1.0.0")
      .progressHandler(progress::add)
      .build();

    client.initialize().compose(init -> {
      testContext.verify(() -> assertThat(init.serverInfo().name()).isEqualTo("weather"));
      return client.listAllTools();
    }).compose(tools -> {
      testContext.verify(() -> assertThat(tools).extracting(McpSchema.Tool::name)
        .containsExactlyInAnyOrder("forecast", "wind-chill"));
      return client.callTool(McpSchema.CallToolRequest.builder()
        .name("forecast")
        .arguments(Map.of("city", "Paris"))
        .meta(Map.of("progressToken", "p-1"))
        .build());
    }).compose(forecast -> {
      testContext.verify(() -> {
        assertThat(text(forecast)).startsWith("Paris:");
        assertThat(progress).hasSize(3);
      });
      return client.callTool(
          new McpSchema.CallToolRequest("wind-chill", Map.of("celsius", -5, "windKph", 30)));
    }).compose(chill -> {
      testContext.verify(() -> assertThat(Double.parseDouble(text(chill))).isLessThan(-5.0));
      return client.readResource(WeatherServerVerticle.STATIONS_URI);
    }).compose(stations -> {
      testContext.verify(() -> assertThat(resourceText(stations)).contains("EGLL"));
      return client.readResource("weather://forecast/Berlin");
    }).compose(templated -> {
      testContext.verify(() -> assertThat(resourceText(templated)).startsWith("Berlin:"));
      return client.getPrompt(new McpSchema.GetPromptRequest("travel-advice", Map.of("city", "Oslo")));
    }).eventually(client::close).onComplete(testContext.succeeding(prompt -> testContext.verify(() -> {
      assertThat(((McpSchema.TextContent) prompt.messages().get(0).content()).text()).contains("Oslo");
      testContext.completeNow();
    })));
  }

  @Test
  void theClientExampleRunsAgainstTheWeatherServer(Vertx vertx, VertxTestContext testContext) {
    List<String> printed = new CopyOnWriteArrayList<>();
    ClientExample.run(vertx, this.baseUrl, printed::add)
      .onComplete(testContext.succeeding(v -> testContext.verify(() -> {
        assertThat(printed).anyMatch(line -> line.startsWith("connected to weather 1.0.0"));
        assertThat(printed).anyMatch(line -> line.startsWith("tool: forecast"));
        assertThat(printed).anyMatch(line -> line.startsWith("tool: wind-chill"));
        assertThat(printed).anyMatch(line -> line.startsWith("forecast: Paris:"));
        assertThat(printed).filteredOn(line -> line.startsWith("  ..")).hasSize(3);
        testContext.completeNow();
      })));
  }

  private static String text(McpSchema.CallToolResult result) {
    return ((McpSchema.TextContent) result.content().get(0)).text();
  }

  private static String resourceText(McpSchema.ReadResourceResult result) {
    return ((McpSchema.TextResourceContents) result.contents().get(0)).text();
  }
}
