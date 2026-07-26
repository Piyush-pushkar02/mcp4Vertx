package io.vertx.mcp.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.modelcontextprotocol.util.DefaultMcpUriTemplateManagerFactory;
import io.modelcontextprotocol.util.McpUriTemplateManagerFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.McpJson;
import io.vertx.mcp.McpStructuredOutputValidator;
import io.vertx.mcp.session.McpServerSession;
import io.vertx.mcp.session.McpStreamableServerSession;
import io.vertx.mcp.spec.EventStore;
import io.vertx.mcp.spec.InMemoryEventStore;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpErrors;
import io.vertx.mcp.spec.McpHandlers;
import io.vertx.mcp.spec.McpPeerInfo;
import io.vertx.mcp.spec.McpRequestContext;
import io.vertx.mcp.spec.McpSessionFactory;
import io.vertx.mcp.spec.McpStatelessHandler;
import io.vertx.mcp.spec.McpStreamableSessionFactory;
import io.vertx.mcp.spec.McpTransports.McpServerTransport;

/**
 * An MCP server: a registry of tools, resources and prompts plus the JSON-RPC routing
 * that exposes them. Transport-agnostic -- attach it to a
 * {@link io.vertx.mcp.server.transport.StreamableHttpServerTransport},
 * {@link io.vertx.mcp.server.transport.SseServerTransport},
 * {@link io.vertx.mcp.server.transport.StatelessHttpServerTransport} or
 * {@link io.vertx.mcp.server.transport.StdioServerTransport}, or several at once.
 *
 * <pre>{@code
 * VertxMcpServer server = VertxMcpServer.builder(vertx)
 *     .serverInfo("weather", "1.0.0")
 *     .tool(forecastTool, (exchange, request) -> {
 *       exchange.progress(0.5, 1.0, "calling upstream");
 *       return webClient.get(...).send().map(this::toResult);
 *     })
 *     .build();
 *
 * router.route("/mcp*").subRouter(
 *     StreamableHttpServerTransport.create(vertx, server).router());
 * }</pre>
 */
public final class VertxMcpServer {

  private static final Logger logger = LoggerFactory.getLogger(VertxMcpServer.class);

  private final Vertx vertx;

  private final McpSchema.Implementation serverInfo;

  private final McpSchema.ServerCapabilities capabilities;

  private final String instructions;

  private final Duration requestTimeout;

  private final JsonRpcCodec codec;

  private final EventStore eventStore;

  private final int pageSize;

  private final McpUriTemplateManagerFactory uriTemplateFactory;

  private final McpStructuredOutputValidator validator;

  private final Map<String, McpServerFeatures.ToolSpec> tools = new LinkedHashMap<>();

  private final Map<String, McpServerFeatures.ResourceSpec> resources = new LinkedHashMap<>();

  private final List<McpServerFeatures.ResourceTemplateSpec> resourceTemplates = new ArrayList<>();

  private final Map<String, McpServerFeatures.PromptSpec> prompts = new LinkedHashMap<>();

  private final Map<String, McpServerFeatures.CompletionSpec> completions = new LinkedHashMap<>();

  private final Map<String, McpHandlers.RequestHandler> requestHandlers = new HashMap<>();

  private final Map<String, McpHandlers.NotificationHandler> notificationHandlers = new HashMap<>();

  /** Transports register here so list-changed notifications reach every connected client. */
  private final List<Broadcaster> broadcasters = new CopyOnWriteArrayList<>();

  private VertxMcpServer(Builder builder) {
    this.vertx = builder.vertx;
    this.serverInfo = builder.serverInfo;
    this.instructions = builder.instructions;
    this.requestTimeout = builder.requestTimeout;
    this.codec = builder.codec != null ? builder.codec : new JsonRpcCodec(McpJson.defaultMapper());
    this.eventStore = builder.eventStore != null ? builder.eventStore : new InMemoryEventStore();
    this.pageSize = builder.pageSize;
    this.uriTemplateFactory = builder.uriTemplateFactory;
    this.validator = builder.validator != null ? builder.validator
        : McpStructuredOutputValidator.defaultValidator();
    builder.tools.forEach(spec -> this.tools.put(spec.tool().name(), spec));
    builder.resources.forEach(spec -> this.resources.put(spec.resource().uri(), spec));
    this.resourceTemplates.addAll(builder.resourceTemplates);
    builder.prompts.forEach(spec -> this.prompts.put(spec.prompt().name(), spec));
    builder.completions.forEach(spec -> this.completions.put(referenceKey(spec.reference()), spec));
    this.capabilities = builder.capabilities != null ? builder.capabilities : deriveCapabilities();
    registerHandlers();
  }

  public static Builder builder(Vertx vertx) {
    return new Builder(vertx);
  }

  // --- accessors used by transports ----------------------------------------

  public Vertx vertx() {
    return this.vertx;
  }

  public JsonRpcCodec codec() {
    return this.codec;
  }

  public McpSchema.Implementation serverInfo() {
    return this.serverInfo;
  }

  public McpSchema.ServerCapabilities capabilities() {
    return this.capabilities;
  }

  public String instructions() {
    return this.instructions;
  }

  public Duration requestTimeout() {
    return this.requestTimeout;
  }

  public EventStore eventStore() {
    return this.eventStore;
  }

  public Map<String, McpHandlers.RequestHandler> requestHandlers() {
    return Map.copyOf(this.requestHandlers);
  }

  public Map<String, McpHandlers.NotificationHandler> notificationHandlers() {
    return Map.copyOf(this.notificationHandlers);
  }

  /** A transport calls this to have list-changed notifications routed to its clients. */
  public void registerBroadcaster(Broadcaster broadcaster) {
    this.broadcasters.add(broadcaster);
  }

  public void unregisterBroadcaster(Broadcaster broadcaster) {
    this.broadcasters.remove(broadcaster);
  }

  // --- session factories ---------------------------------------------------

  /** Factory for streamable-HTTP sessions. */
  public McpStreamableSessionFactory streamableSessionFactory() {
    return initializeRequest -> {
      String protocolVersion = negotiateProtocolVersion(initializeRequest.protocolVersion());
      McpStreamableServerSession session = new McpStreamableServerSession(this.vertx,
          McpStreamableServerSession.newSessionId(), this.codec, this.requestTimeout, this.requestHandlers,
          this.notificationHandlers, this.eventStore);
      session.clientInfo(initializeRequest.capabilities(), initializeRequest.clientInfo(), protocolVersion);
      McpSchema.InitializeResult result = new McpSchema.InitializeResult(protocolVersion, this.capabilities,
          this.serverInfo, this.instructions, null);
      return Future.succeededFuture(new McpStreamableSessionFactory.InitResult(session, result));
    };
  }

  /** Factory for single-stream sessions (legacy SSE, stdio). */
  public McpSessionFactory sessionFactory() {
    return (sessionId, transport) -> newServerSession(sessionId, transport);
  }

  public McpServerSession newServerSession(String sessionId, McpServerTransport transport) {
    return new McpServerSession(this.vertx, sessionId, transport, this.codec, this.requestTimeout,
        this.requestHandlers, this.notificationHandlers);
  }

  /** Handler for the stateless transport: no session, no server-to-client traffic. */
  public McpStatelessHandler statelessHandler() {
    return new StatelessHandler(this);
  }

  String negotiateProtocolVersion(String requested) {
    List<String> supported = List.of(ProtocolVersions.MCP_2024_11_05, ProtocolVersions.MCP_2025_03_26,
        ProtocolVersions.MCP_2025_06_18, ProtocolVersions.MCP_2025_11_25);
    if (requested != null && supported.contains(requested)) {
      return requested;
    }
    // Spec: if we do not support what the client asked for, answer with our latest and
    // let the client decide whether to continue.
    return ProtocolVersions.MCP_2025_06_18;
  }

  // --- dynamic registry ----------------------------------------------------

  public Future<Void> addTool(McpSchema.Tool tool, McpServerFeatures.ToolHandler handler) {
    this.tools.put(tool.name(), new McpServerFeatures.ToolSpec(tool, handler));
    return notifyToolsListChanged();
  }

  public Future<Void> removeTool(String name) {
    return this.tools.remove(name) != null ? notifyToolsListChanged() : Future.succeededFuture();
  }

  public Future<Void> addResource(McpSchema.Resource resource, McpServerFeatures.ResourceHandler handler) {
    this.resources.put(resource.uri(), new McpServerFeatures.ResourceSpec(resource, handler));
    return notifyResourcesListChanged();
  }

  public Future<Void> removeResource(String uri) {
    return this.resources.remove(uri) != null ? notifyResourcesListChanged() : Future.succeededFuture();
  }

  public Future<Void> addPrompt(McpSchema.Prompt prompt, McpServerFeatures.PromptHandler handler) {
    this.prompts.put(prompt.name(), new McpServerFeatures.PromptSpec(prompt, handler));
    return notifyPromptsListChanged();
  }

  public Future<Void> removePrompt(String name) {
    return this.prompts.remove(name) != null ? notifyPromptsListChanged() : Future.succeededFuture();
  }

  public Future<Void> notifyToolsListChanged() {
    return broadcast(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, null);
  }

  public Future<Void> notifyResourcesListChanged() {
    return broadcast(McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED, null);
  }

  public Future<Void> notifyPromptsListChanged() {
    return broadcast(McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED, null);
  }

  public Future<Void> notifyResourceUpdated(String uri) {
    return broadcast(McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED, Map.of("uri", uri));
  }

  private Future<Void> broadcast(String method, Object params) {
    List<Future<?>> sends = new ArrayList<>();
    for (Broadcaster broadcaster : this.broadcasters) {
      sends.add(broadcaster.notifyClients(method, params).recover(err -> {
        logger.debug("Broadcast of {} failed on one transport", method, err);
        return Future.succeededFuture();
      }));
    }
    return Future.all(sends).mapEmpty();
  }

  // --- handler wiring ------------------------------------------------------

  private void registerHandlers() {
    this.requestHandlers.put(McpSchema.METHOD_INITIALIZE, this::handleInitialize);
    this.requestHandlers.put(McpSchema.METHOD_PING, ctx -> Future.succeededFuture(Map.of()));

    if (this.capabilities.tools() != null) {
      this.requestHandlers.put(McpSchema.METHOD_TOOLS_LIST, this::handleToolsList);
      this.requestHandlers.put(McpSchema.METHOD_TOOLS_CALL, this::handleToolsCall);
    }
    if (this.capabilities.resources() != null) {
      this.requestHandlers.put(McpSchema.METHOD_RESOURCES_LIST, this::handleResourcesList);
      this.requestHandlers.put(McpSchema.METHOD_RESOURCES_READ, this::handleResourcesRead);
      this.requestHandlers.put(McpSchema.METHOD_RESOURCES_TEMPLATES_LIST, this::handleResourceTemplatesList);
    }
    if (this.capabilities.prompts() != null) {
      this.requestHandlers.put(McpSchema.METHOD_PROMPT_LIST, this::handlePromptsList);
      this.requestHandlers.put(McpSchema.METHOD_PROMPT_GET, this::handlePromptGet);
    }
    if (this.capabilities.completions() != null) {
      this.requestHandlers.put(McpSchema.METHOD_COMPLETION_COMPLETE, this::handleComplete);
    }
    if (this.capabilities.logging() != null) {
      this.requestHandlers.put(McpSchema.METHOD_LOGGING_SET_LEVEL, this::handleSetLevel);
    }

    this.notificationHandlers.put(McpSchema.METHOD_NOTIFICATION_INITIALIZED,
        ctx -> Future.succeededFuture());
    this.notificationHandlers.put(McpSchema.METHOD_NOTIFICATION_PROGRESS, ctx -> Future.succeededFuture());
    this.notificationHandlers.put(McpSchema.METHOD_NOTIFICATION_ROOTS_LIST_CHANGED,
        ctx -> Future.succeededFuture());
  }

  private Future<Object> handleInitialize(McpRequestContext ctx) {
    McpSchema.InitializeRequest request = this.codec.unmarshal(ctx.params(), McpSchema.InitializeRequest.class);
    String version = negotiateProtocolVersion(request.protocolVersion());
    if (ctx.session() instanceof McpServerSession session) {
      session.clientInfo(request.capabilities(), request.clientInfo());
      session.markInitialized();
    }
    return Future.succeededFuture(
        new McpSchema.InitializeResult(version, this.capabilities, this.serverInfo, this.instructions, null));
  }

  private Future<Object> handleToolsList(McpRequestContext ctx) {
    List<McpSchema.Tool> all = this.tools.values().stream().map(McpServerFeatures.ToolSpec::tool).toList();
    Page<McpSchema.Tool> page = paginate(all, cursorOf(ctx));
    return Future.succeededFuture(new McpSchema.ListToolsResult(page.items(), page.nextCursor(), null));
  }

  private Future<Object> handleToolsCall(McpRequestContext ctx) {
    McpSchema.CallToolRequest request = this.codec.unmarshal(ctx.params(), McpSchema.CallToolRequest.class);
    McpServerFeatures.ToolSpec spec = this.tools.get(request.name());
    if (spec == null) {
      return Future.failedFuture(McpErrors.invalidParams("Unknown tool: " + request.name()));
    }
    return spec.handler().handle(exchange(ctx), request).compose(result -> validateOutput(spec, result)).map(r -> r);
  }

  private Future<McpSchema.CallToolResult> validateOutput(McpServerFeatures.ToolSpec spec,
      McpSchema.CallToolResult result) {
    Map<String, Object> outputSchema = spec.tool().outputSchema();
    if (outputSchema == null || Boolean.TRUE.equals(result.isError())) {
      return Future.succeededFuture(result);
    }
    var response = this.validator.validate(outputSchema, result.structuredContent());
    if (!response.valid()) {
      return Future.failedFuture(McpErrors
        .internalError("Tool '" + spec.tool().name() + "' returned output that violates its outputSchema: "
            + response.errorMessage()));
    }
    return Future.succeededFuture(result);
  }

  private Future<Object> handleResourcesList(McpRequestContext ctx) {
    List<McpSchema.Resource> all = this.resources.values()
      .stream()
      .map(McpServerFeatures.ResourceSpec::resource)
      .toList();
    Page<McpSchema.Resource> page = paginate(all, cursorOf(ctx));
    return Future.succeededFuture(new McpSchema.ListResourcesResult(page.items(), page.nextCursor(), null));
  }

  private Future<Object> handleResourceTemplatesList(McpRequestContext ctx) {
    List<McpSchema.ResourceTemplate> all = this.resourceTemplates.stream()
      .map(McpServerFeatures.ResourceTemplateSpec::template)
      .toList();
    Page<McpSchema.ResourceTemplate> page = paginate(all, cursorOf(ctx));
    return Future.succeededFuture(new McpSchema.ListResourceTemplatesResult(page.items(), page.nextCursor(), null));
  }

  private Future<Object> handleResourcesRead(McpRequestContext ctx) {
    McpSchema.ReadResourceRequest request = this.codec.unmarshal(ctx.params(), McpSchema.ReadResourceRequest.class);
    McpServerFeatures.ResourceSpec exact = this.resources.get(request.uri());
    if (exact != null) {
      return exact.handler().handle(exchange(ctx), request).map(r -> r);
    }
    for (McpServerFeatures.ResourceTemplateSpec templateSpec : this.resourceTemplates) {
      if (this.uriTemplateFactory.create(templateSpec.template().uriTemplate()).matches(request.uri())) {
        return templateSpec.handler().handle(exchange(ctx), request).map(r -> r);
      }
    }
    return Future.failedFuture(McpErrors.resourceNotFound(request.uri()));
  }

  private Future<Object> handlePromptsList(McpRequestContext ctx) {
    List<McpSchema.Prompt> all = this.prompts.values().stream().map(McpServerFeatures.PromptSpec::prompt).toList();
    Page<McpSchema.Prompt> page = paginate(all, cursorOf(ctx));
    return Future.succeededFuture(new McpSchema.ListPromptsResult(page.items(), page.nextCursor(), null));
  }

  private Future<Object> handlePromptGet(McpRequestContext ctx) {
    McpSchema.GetPromptRequest request = this.codec.unmarshal(ctx.params(), McpSchema.GetPromptRequest.class);
    McpServerFeatures.PromptSpec spec = this.prompts.get(request.name());
    if (spec == null) {
      return Future.failedFuture(McpErrors.invalidParams("Unknown prompt: " + request.name()));
    }
    return spec.handler().handle(exchange(ctx), request).map(r -> r);
  }

  private Future<Object> handleComplete(McpRequestContext ctx) {
    McpSchema.CompleteRequest request = this.codec.unmarshal(ctx.params(), McpSchema.CompleteRequest.class);
    McpServerFeatures.CompletionSpec spec = this.completions.get(referenceKey(request.ref()));
    if (spec == null) {
      return Future.succeededFuture(
          new McpSchema.CompleteResult(new McpSchema.CompleteResult.CompleteCompletion(List.of(), 0, false), null));
    }
    return spec.handler().handle(exchange(ctx), request).map(r -> r);
  }

  private Future<Object> handleSetLevel(McpRequestContext ctx) {
    McpSchema.SetLevelRequest request = this.codec.unmarshal(ctx.params(), McpSchema.SetLevelRequest.class);
    ctx.session().setMinLoggingLevel(request.level());
    return Future.succeededFuture(Map.of());
  }

  McpServerExchange exchange(McpRequestContext ctx) {
    McpSchema.ClientCapabilities caps = null;
    McpSchema.Implementation info = null;
    if (ctx.session() instanceof McpPeerInfo peer) {
      caps = peer.clientCapabilities();
      info = peer.clientInfo();
    }
    return new McpServerExchange(ctx.session(), ctx.sessionId(), caps, info, ctx.transportContext(),
        ctx.progressToken());
  }

  // --- pagination ----------------------------------------------------------

  private String cursorOf(McpRequestContext ctx) {
    if (ctx.params() instanceof Map<?, ?> map) {
      Object cursor = map.get("cursor");
      return cursor != null ? cursor.toString() : null;
    }
    return null;
  }

  private <T> Page<T> paginate(List<T> all, String cursor) {
    if (this.pageSize <= 0) {
      return new Page<>(all, null);
    }
    int offset = 0;
    if (cursor != null && !cursor.isBlank()) {
      try {
        offset = Integer.parseInt(new String(Base64.getUrlDecoder().decode(cursor)));
      }
      catch (RuntimeException e) {
        throw McpErrors.invalidParams("Invalid cursor: " + cursor);
      }
    }
    if (offset >= all.size()) {
      return new Page<>(List.of(), null);
    }
    int end = Math.min(offset + this.pageSize, all.size());
    String next = end < all.size() ? Base64.getUrlEncoder().withoutPadding().encodeToString(String.valueOf(end).getBytes())
        : null;
    return new Page<>(all.subList(offset, end), next);
  }

  private record Page<T>(List<T> items, String nextCursor) {
  }

  private McpSchema.ServerCapabilities deriveCapabilities() {
    return new McpSchema.ServerCapabilities(
        this.completions.isEmpty() ? null : new McpSchema.ServerCapabilities.CompletionCapabilities(), null,
        new McpSchema.ServerCapabilities.LoggingCapabilities(),
        this.prompts.isEmpty() ? null : new McpSchema.ServerCapabilities.PromptCapabilities(true),
        this.resources.isEmpty() && this.resourceTemplates.isEmpty() ? null
            : new McpSchema.ServerCapabilities.ResourceCapabilities(false, true),
        this.tools.isEmpty() ? null : new McpSchema.ServerCapabilities.ToolCapabilities(true));
  }

  private static String referenceKey(McpSchema.CompleteReference reference) {
    if (reference instanceof McpSchema.PromptReference prompt) {
      return "prompt:" + prompt.name();
    }
    if (reference instanceof McpSchema.ResourceReference resource) {
      return "resource:" + resource.uri();
    }
    return String.valueOf(reference);
  }

  /** How a transport pushes an unsolicited notification to all of its sessions. */
  @FunctionalInterface
  public interface Broadcaster {
    Future<Void> notifyClients(String method, Object params);
  }

  // --- builder -------------------------------------------------------------

  public static final class Builder {

    private final Vertx vertx;

    private McpSchema.Implementation serverInfo = new McpSchema.Implementation("vertx-mcp-server", "0.1.0");

    private McpSchema.ServerCapabilities capabilities;

    private String instructions;

    private Duration requestTimeout = Duration.ofSeconds(30);

    private JsonRpcCodec codec;

    private EventStore eventStore;

    private int pageSize = 0;

    private McpUriTemplateManagerFactory uriTemplateFactory = new DefaultMcpUriTemplateManagerFactory();

    private McpStructuredOutputValidator validator;

    private final List<McpServerFeatures.ToolSpec> tools = new ArrayList<>();

    private final List<McpServerFeatures.ResourceSpec> resources = new ArrayList<>();

    private final List<McpServerFeatures.ResourceTemplateSpec> resourceTemplates = new ArrayList<>();

    private final List<McpServerFeatures.PromptSpec> prompts = new ArrayList<>();

    private final List<McpServerFeatures.CompletionSpec> completions = new ArrayList<>();

    private Builder(Vertx vertx) {
      this.vertx = vertx;
    }

    public Builder serverInfo(String name, String version) {
      this.serverInfo = new McpSchema.Implementation(name, version);
      return this;
    }

    public Builder serverInfo(McpSchema.Implementation serverInfo) {
      this.serverInfo = serverInfo;
      return this;
    }

    /** Override the capabilities advertised at initialize. Derived from what you register otherwise. */
    public Builder capabilities(McpSchema.ServerCapabilities capabilities) {
      this.capabilities = capabilities;
      return this;
    }

    public Builder instructions(String instructions) {
      this.instructions = instructions;
      return this;
    }

    /** How long to wait for a client's answer to a server-initiated request. */
    public Builder requestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    /** Where SSE events are recorded for {@code Last-Event-ID} resumption. */
    public Builder eventStore(EventStore eventStore) {
      this.eventStore = eventStore;
      return this;
    }

    /** Items per page for list operations. Zero (default) returns everything in one page. */
    public Builder pageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public Builder uriTemplateFactory(McpUriTemplateManagerFactory factory) {
      this.uriTemplateFactory = factory;
      return this;
    }

    public Builder structuredOutputValidator(McpStructuredOutputValidator validator) {
      this.validator = validator;
      return this;
    }

    public Builder tool(McpSchema.Tool tool, McpServerFeatures.ToolHandler handler) {
      this.tools.add(new McpServerFeatures.ToolSpec(tool, handler));
      return this;
    }

    /**
     * Registers a tool whose handler blocks. It runs on a Vert.x worker thread via
     * {@code executeBlocking} so the event loop is never parked; the returned future
     * completes back on the calling context.
     */
    public Builder toolBlocking(McpSchema.Tool tool, McpServerFeatures.BlockingToolHandler handler) {
      return tool(tool, (exchange, request) -> this.vertx.executeBlocking(() -> handler.handle(exchange, request)));
    }

    public Builder resource(McpSchema.Resource resource, McpServerFeatures.ResourceHandler handler) {
      this.resources.add(new McpServerFeatures.ResourceSpec(resource, handler));
      return this;
    }

    public Builder resourceBlocking(McpSchema.Resource resource, McpServerFeatures.BlockingResourceHandler handler) {
      return resource(resource,
          (exchange, request) -> this.vertx.executeBlocking(() -> handler.handle(exchange, request)));
    }

    public Builder resourceTemplate(McpSchema.ResourceTemplate template,
        McpServerFeatures.ResourceHandler handler) {
      this.resourceTemplates.add(new McpServerFeatures.ResourceTemplateSpec(template, handler));
      return this;
    }

    public Builder prompt(McpSchema.Prompt prompt, McpServerFeatures.PromptHandler handler) {
      this.prompts.add(new McpServerFeatures.PromptSpec(prompt, handler));
      return this;
    }

    public Builder promptBlocking(McpSchema.Prompt prompt, McpServerFeatures.BlockingPromptHandler handler) {
      return prompt(prompt,
          (exchange, request) -> this.vertx.executeBlocking(() -> handler.handle(exchange, request)));
    }

    public Builder completion(McpSchema.CompleteReference reference,
        McpServerFeatures.CompletionHandler handler) {
      this.completions.add(new McpServerFeatures.CompletionSpec(reference, handler));
      return this;
    }

    public VertxMcpServer build() {
      this.tools.sort(Comparator.comparing(spec -> spec.tool().name()));
      return new VertxMcpServer(this);
    }
  }

  /** Adapts the request handlers to the stateless transport, where there is no session. */
  private record StatelessHandler(VertxMcpServer server) implements McpStatelessHandler {

    @Override
    public Future<McpSchema.JSONRPCResponse> handleRequest(io.vertx.mcp.McpTransportContext context,
        McpSchema.JSONRPCRequest request) {
      McpHandlers.RequestHandler handler = this.server.requestHandlers.get(request.method());
      if (handler == null) {
        return Future
          .succeededFuture(McpErrors.errorResponse(request.id(), McpErrors.methodNotFound(request.method())));
      }
      McpRequestContext ctx = McpRequestContext.of(NoOpSession.INSTANCE, null, context, request.params(),
          request.id());
      Future<Object> result;
      try {
        result = handler.handle(ctx);
      }
      catch (Throwable t) {
        result = Future.failedFuture(t);
      }
      return result
        .map(value -> new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), value, null))
        .otherwise((Function<Throwable, McpSchema.JSONRPCResponse>) err -> McpErrors.errorResponse(request.id(),
            err));
    }

    @Override
    public Future<Void> handleNotification(io.vertx.mcp.McpTransportContext context,
        McpSchema.JSONRPCNotification notification) {
      McpHandlers.NotificationHandler handler = this.server.notificationHandlers.get(notification.method());
      if (handler == null) {
        return Future.succeededFuture();
      }
      return handler
        .handle(McpRequestContext.of(NoOpSession.INSTANCE, null, context, notification.params(), null))
        .recover(err -> Future.succeededFuture());
    }
  }

  /**
   * The session handed to handlers in stateless mode. Server-to-client traffic is
   * impossible there, so it fails loudly rather than silently swallowing a sampling
   * request a handler thought went through.
   */
  private enum NoOpSession implements io.vertx.mcp.spec.McpSession {

    INSTANCE;

    @Override
    public <T> Future<T> sendRequest(String method, Object params,
        io.modelcontextprotocol.json.TypeRef<T> resultType) {
      return Future.failedFuture(new UnsupportedOperationException(
          "'" + method + "' requires a session; this server is running on the stateless transport"));
    }

    @Override
    public Future<Void> sendNotification(String method, Object params) {
      return Future.succeededFuture();
    }

    @Override
    public Future<Void> close() {
      return Future.succeededFuture();
    }
  }
}
