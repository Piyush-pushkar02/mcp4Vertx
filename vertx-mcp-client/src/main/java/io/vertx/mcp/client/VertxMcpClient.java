package io.vertx.mcp.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse.JSONRPCError;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.mcp.client.transport.McpProtocolVersionAware;
import io.vertx.mcp.session.McpClientSession;
import io.vertx.mcp.spec.JsonRpcCodec;
import io.vertx.mcp.spec.McpHandlers;
import io.vertx.mcp.spec.McpRequestContext;
import io.vertx.mcp.spec.McpTransports.McpClientTransport;

/**
 * An MCP client: the handshake, the twelve request methods a client can send, and the
 * handlers for everything the server sends back unprompted.
 *
 * <p>
 * Every method returns a {@link Future} that completes on the Vert.x context the client
 * was built on. Nothing here blocks, and nothing here is Reactor -- a
 * {@code tools/call} that streams progress notifications delivers them to your progress
 * handler on the same event loop that will later complete the call's future, in order.
 *
 * <pre>{@code
 * VertxMcpClient client = VertxMcpClient.builder(vertx)
 *     .transport(StreamableHttpClientTransport.builder(vertx).baseUrl("http://localhost:8080").build())
 *     .clientInfo("my-app", "1.0.0")
 *     .sampling(request -> myModel.complete(request))
 *     .progressHandler(progress -> logger.info("{}%", progress.progress()))
 *     .build();
 *
 * client.initialize()
 *     .compose(init -> client.listAllTools())
 *     .onSuccess(tools -> tools.forEach(tool -> logger.info(tool.name())));
 * }</pre>
 */
public final class VertxMcpClient {

  private static final Logger logger = LoggerFactory.getLogger(VertxMcpClient.class);

  private static final TypeRef<McpSchema.InitializeResult> INITIALIZE = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.ListToolsResult> LIST_TOOLS = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.CallToolResult> CALL_TOOL = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.ListResourcesResult> LIST_RESOURCES = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.ListResourceTemplatesResult> LIST_RESOURCE_TEMPLATES = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.ReadResourceResult> READ_RESOURCE = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.ListPromptsResult> LIST_PROMPTS = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.GetPromptResult> GET_PROMPT = new TypeRef<>() {
  };

  private static final TypeRef<McpSchema.CompleteResult> COMPLETE = new TypeRef<>() {
  };

  private static final TypeRef<Object> ANY = new TypeRef<>() {
  };

  private final McpClientTransport transport;

  private final McpSchema.Implementation clientInfo;

  private final McpSchema.ClientCapabilities capabilities;

  private final JsonRpcCodec codec;

  private final McpClientSession session;

  private final List<McpSchema.Root> roots;

  private final McpClientFeatures.SamplingHandler samplingHandler;

  private final McpClientFeatures.ElicitationHandler elicitationHandler;

  private final McpClientFeatures.LoggingHandler loggingHandler;

  private final McpClientFeatures.ProgressHandler progressHandler;

  private final McpClientFeatures.ListChangedHandler toolsChangeHandler;

  private final McpClientFeatures.ListChangedHandler resourcesChangeHandler;

  private final McpClientFeatures.ListChangedHandler promptsChangeHandler;

  private final McpClientFeatures.ResourceUpdatedHandler resourceUpdatedHandler;

  private Future<McpSchema.InitializeResult> initialization;

  private McpSchema.InitializeResult initializeResult;

  private VertxMcpClient(Builder builder) {
    this.transport = builder.transport;
    this.clientInfo = builder.clientInfo;
    this.samplingHandler = builder.samplingHandler;
    this.elicitationHandler = builder.elicitationHandler;
    this.loggingHandler = builder.loggingHandler;
    this.progressHandler = builder.progressHandler;
    this.toolsChangeHandler = builder.toolsChangeHandler;
    this.resourcesChangeHandler = builder.resourcesChangeHandler;
    this.promptsChangeHandler = builder.promptsChangeHandler;
    this.resourceUpdatedHandler = builder.resourceUpdatedHandler;
    this.roots = new ArrayList<>(builder.roots);
    this.capabilities = builder.capabilities != null ? builder.capabilities : deriveCapabilities();
    this.codec = builder.codec != null ? builder.codec : JsonRpcCodec.defaultCodec();

    Map<String, McpHandlers.RequestHandler> requestHandlers = new HashMap<>();
    Map<String, McpHandlers.NotificationHandler> notificationHandlers = new HashMap<>();
    registerHandlers(requestHandlers, notificationHandlers);
    String sessionId = builder.sessionId != null ? builder.sessionId
        : UUID.randomUUID().toString().substring(0, 8);
    this.session = new McpClientSession(builder.vertx, sessionId, this.transport, this.codec,
        builder.requestTimeout, requestHandlers, notificationHandlers);
  }

  public static Builder builder(Vertx vertx) {
    return new Builder(vertx);
  }

  // --- accessors -----------------------------------------------------------

  /** The underlying session, for anything this API does not wrap. */
  public McpClientSession session() {
    return this.session;
  }

  public McpSchema.Implementation clientInfo() {
    return this.clientInfo;
  }

  public McpSchema.ClientCapabilities clientCapabilities() {
    return this.capabilities;
  }

  /** The server's handshake response, or {@code null} before {@link #initialize()}. */
  public McpSchema.InitializeResult initializeResult() {
    return this.initializeResult;
  }

  public McpSchema.ServerCapabilities serverCapabilities() {
    return this.initializeResult != null ? this.initializeResult.capabilities() : null;
  }

  public McpSchema.Implementation serverInfo() {
    return this.initializeResult != null ? this.initializeResult.serverInfo() : null;
  }

  /** The protocol version both ends agreed on, or {@code null} before initialize. */
  public String protocolVersion() {
    return this.initializeResult != null ? this.initializeResult.protocolVersion() : null;
  }

  public boolean isInitialized() {
    return this.initializeResult != null;
  }

  // --- lifecycle -----------------------------------------------------------

  /**
   * Connects the transport, performs the {@code initialize} handshake, then sends
   * {@code notifications/initialized}. Repeat calls return the same future, so it is safe
   * to treat as an idempotent "make sure we are ready".
   */
  public Future<McpSchema.InitializeResult> initialize() {
    if (this.initialization != null) {
      return this.initialization;
    }
    List<String> supported = this.transport.protocolVersions();
    String preferred = supported.get(supported.size() - 1);
    this.initialization = this.session.connect()
      .compose(connected -> this.session.sendRequest(McpSchema.METHOD_INITIALIZE,
          new McpSchema.InitializeRequest(preferred, this.capabilities, this.clientInfo), INITIALIZE))
      .compose(result -> {
        if (!supported.contains(result.protocolVersion())) {
          // The specification is explicit: a client that cannot speak the version the
          // server picked must disconnect rather than guess.
          return Future.failedFuture(new IllegalStateException("Server chose protocol version '"
              + result.protocolVersion() + "', but this client only speaks " + supported));
        }
        this.initializeResult = result;
        if (this.transport instanceof McpProtocolVersionAware aware) {
          aware.negotiatedProtocolVersion(result.protocolVersion());
        }
        // The server may not answer anything else until it has seen this.
        return this.session.sendNotification(McpSchema.METHOD_NOTIFICATION_INITIALIZED).map(result);
      });
    return this.initialization;
  }

  public Future<Void> close() {
    return this.session.close();
  }

  // --- requests ------------------------------------------------------------

  /** Round-trips a {@code ping}. Useful as a liveness probe on an idle connection. */
  public Future<Void> ping() {
    return request(McpSchema.METHOD_PING, Map.of(), ANY).mapEmpty();
  }

  public Future<McpSchema.ListToolsResult> listTools() {
    return listTools(null);
  }

  public Future<McpSchema.ListToolsResult> listTools(String cursor) {
    return request(McpSchema.METHOD_TOOLS_LIST, new McpSchema.PaginatedRequest(cursor), LIST_TOOLS);
  }

  /** Walks {@code nextCursor} to the end and returns every tool the server exposes. */
  public Future<List<McpSchema.Tool>> listAllTools() {
    return collect(this::listTools, McpSchema.ListToolsResult::tools, McpSchema.ListToolsResult::nextCursor);
  }

  public Future<McpSchema.CallToolResult> callTool(McpSchema.CallToolRequest request) {
    return request(McpSchema.METHOD_TOOLS_CALL, request, CALL_TOOL);
  }

  public Future<McpSchema.ListResourcesResult> listResources() {
    return listResources(null);
  }

  public Future<McpSchema.ListResourcesResult> listResources(String cursor) {
    return request(McpSchema.METHOD_RESOURCES_LIST, new McpSchema.PaginatedRequest(cursor), LIST_RESOURCES);
  }

  public Future<List<McpSchema.Resource>> listAllResources() {
    return collect(this::listResources, McpSchema.ListResourcesResult::resources,
        McpSchema.ListResourcesResult::nextCursor);
  }

  public Future<McpSchema.ReadResourceResult> readResource(McpSchema.ReadResourceRequest request) {
    return request(McpSchema.METHOD_RESOURCES_READ, request, READ_RESOURCE);
  }

  public Future<McpSchema.ReadResourceResult> readResource(String uri) {
    return readResource(new McpSchema.ReadResourceRequest(uri));
  }

  public Future<McpSchema.ListResourceTemplatesResult> listResourceTemplates() {
    return listResourceTemplates(null);
  }

  public Future<McpSchema.ListResourceTemplatesResult> listResourceTemplates(String cursor) {
    return request(McpSchema.METHOD_RESOURCES_TEMPLATES_LIST, new McpSchema.PaginatedRequest(cursor),
        LIST_RESOURCE_TEMPLATES);
  }

  public Future<List<McpSchema.ResourceTemplate>> listAllResourceTemplates() {
    return collect(this::listResourceTemplates, McpSchema.ListResourceTemplatesResult::resourceTemplates,
        McpSchema.ListResourceTemplatesResult::nextCursor);
  }

  /** Asks for {@code notifications/resources/updated} whenever this resource changes. */
  public Future<Void> subscribeResource(String uri) {
    return request(McpSchema.METHOD_RESOURCES_SUBSCRIBE, new McpSchema.SubscribeRequest(uri), ANY).mapEmpty();
  }

  public Future<Void> unsubscribeResource(String uri) {
    return request(McpSchema.METHOD_RESOURCES_UNSUBSCRIBE, new McpSchema.UnsubscribeRequest(uri), ANY).mapEmpty();
  }

  public Future<McpSchema.ListPromptsResult> listPrompts() {
    return listPrompts(null);
  }

  public Future<McpSchema.ListPromptsResult> listPrompts(String cursor) {
    return request(McpSchema.METHOD_PROMPT_LIST, new McpSchema.PaginatedRequest(cursor), LIST_PROMPTS);
  }

  public Future<List<McpSchema.Prompt>> listAllPrompts() {
    return collect(this::listPrompts, McpSchema.ListPromptsResult::prompts, McpSchema.ListPromptsResult::nextCursor);
  }

  public Future<McpSchema.GetPromptResult> getPrompt(McpSchema.GetPromptRequest request) {
    return request(McpSchema.METHOD_PROMPT_GET, request, GET_PROMPT);
  }

  public Future<McpSchema.CompleteResult> completeCompletion(McpSchema.CompleteRequest request) {
    return request(McpSchema.METHOD_COMPLETION_COMPLETE, request, COMPLETE);
  }

  /** Sets the minimum severity of {@code notifications/message} the server should send. */
  public Future<Void> setLoggingLevel(McpSchema.LoggingLevel level) {
    return request(McpSchema.METHOD_LOGGING_SET_LEVEL, new McpSchema.SetLevelRequest(level), ANY).mapEmpty();
  }

  // --- roots ---------------------------------------------------------------

  /** The roots this client exposes, as answered to {@code roots/list}. */
  public List<McpSchema.Root> roots() {
    return List.copyOf(this.roots);
  }

  /**
   * Adds a root and tells the server the list moved. The notification is only sent once
   * initialize has completed -- before that the server has nothing to invalidate.
   */
  public Future<Void> addRoot(McpSchema.Root root) {
    this.roots.removeIf(existing -> existing.uri().equals(root.uri()));
    this.roots.add(root);
    return isInitialized() ? rootsListChangedNotification() : Future.succeededFuture();
  }

  public Future<Void> removeRoot(String uri) {
    if (!this.roots.removeIf(existing -> existing.uri().equals(uri))) {
      return Future.succeededFuture();
    }
    return isInitialized() ? rootsListChangedNotification() : Future.succeededFuture();
  }

  public Future<Void> rootsListChangedNotification() {
    return this.session.sendNotification(McpSchema.METHOD_NOTIFICATION_ROOTS_LIST_CHANGED);
  }

  // --- inbound handlers ----------------------------------------------------

  private void registerHandlers(Map<String, McpHandlers.RequestHandler> requests,
      Map<String, McpHandlers.NotificationHandler> notifications) {
    requests.put(McpSchema.METHOD_PING, context -> Future.succeededFuture(Map.of()));
    requests.put(McpSchema.METHOD_ROOTS_LIST,
        context -> Future.succeededFuture(new McpSchema.ListRootsResult(List.copyOf(this.roots))));
    requests.put(McpSchema.METHOD_SAMPLING_CREATE_MESSAGE, this::handleSampling);
    requests.put(McpSchema.METHOD_ELICITATION_CREATE, this::handleElicitation);

    notifications.put(McpSchema.METHOD_NOTIFICATION_MESSAGE, this::handleLoggingMessage);
    notifications.put(McpSchema.METHOD_NOTIFICATION_PROGRESS, this::handleProgress);
    notifications.put(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED,
        context -> fire(this.toolsChangeHandler));
    notifications.put(McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED,
        context -> fire(this.resourcesChangeHandler));
    notifications.put(McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED,
        context -> fire(this.promptsChangeHandler));
    notifications.put(McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED, this::handleResourceUpdated);
  }

  private Future<Object> handleSampling(McpRequestContext context) {
    if (this.samplingHandler == null) {
      return Future.failedFuture(unsupported("sampling", McpSchema.METHOD_SAMPLING_CREATE_MESSAGE));
    }
    McpSchema.CreateMessageRequest request = this.codec.unmarshal(context.params(),
        McpSchema.CreateMessageRequest.class);
    return this.samplingHandler.handle(request).map(result -> result);
  }

  private Future<Object> handleElicitation(McpRequestContext context) {
    if (this.elicitationHandler == null) {
      return Future.failedFuture(unsupported("elicitation", McpSchema.METHOD_ELICITATION_CREATE));
    }
    McpSchema.ElicitRequest request = this.codec.unmarshal(context.params(), McpSchema.ElicitRequest.class);
    return this.elicitationHandler.handle(request).map(result -> result);
  }

  private Future<Void> handleLoggingMessage(McpRequestContext context) {
    if (this.loggingHandler != null) {
      this.loggingHandler
        .handle(this.codec.unmarshal(context.params(), McpSchema.LoggingMessageNotification.class));
    }
    return Future.succeededFuture();
  }

  private Future<Void> handleProgress(McpRequestContext context) {
    if (this.progressHandler != null) {
      this.progressHandler.handle(this.codec.unmarshal(context.params(), McpSchema.ProgressNotification.class));
    }
    return Future.succeededFuture();
  }

  private Future<Void> handleResourceUpdated(McpRequestContext context) {
    if (this.resourceUpdatedHandler != null) {
      McpSchema.ResourcesUpdatedNotification notification = this.codec.unmarshal(context.params(),
          McpSchema.ResourcesUpdatedNotification.class);
      this.resourceUpdatedHandler.handle(notification.uri());
    }
    return Future.succeededFuture();
  }

  private static Future<Void> fire(McpClientFeatures.ListChangedHandler handler) {
    if (handler != null) {
      handler.handle();
    }
    return Future.succeededFuture();
  }

  private static McpError unsupported(String capability, String method) {
    return new McpError(new JSONRPCError(McpSchema.ErrorCodes.METHOD_NOT_FOUND,
        "This client does not support " + capability + ": no handler is configured for " + method, null));
  }

  // --- plumbing ------------------------------------------------------------

  private <T> Future<T> request(String method, Object params, TypeRef<T> resultType) {
    return whenInitialized(() -> this.session.sendRequest(method, params, resultType), method);
  }

  private <T> Future<T> whenInitialized(Supplier<Future<T>> action, String method) {
    if (this.initializeResult == null) {
      return Future.failedFuture(new IllegalStateException(
          "Cannot send '" + method + "': the client is not initialized. Call initialize() and wait for the "
              + "returned future before sending anything else."));
    }
    return action.get();
  }

  private <R, T> Future<List<T>> collect(Function<String, Future<R>> page, Function<R, List<T>> items,
      Function<R, String> nextCursor) {
    return collectPage(page, items, nextCursor, new ArrayList<>(), null);
  }

  private <R, T> Future<List<T>> collectPage(Function<String, Future<R>> page, Function<R, List<T>> items,
      Function<R, String> nextCursor, List<T> accumulator, String cursor) {
    return page.apply(cursor).compose(result -> {
      List<T> batch = items.apply(result);
      if (batch != null) {
        accumulator.addAll(batch);
      }
      String next = nextCursor.apply(result);
      if (next == null || next.isEmpty() || next.equals(cursor)) {
        // A server that echoes the cursor back unchanged would otherwise loop forever.
        return Future.succeededFuture(accumulator);
      }
      return collectPage(page, items, nextCursor, accumulator, next);
    });
  }

  private McpSchema.ClientCapabilities deriveCapabilities() {
    return new McpSchema.ClientCapabilities(null, new McpSchema.ClientCapabilities.RootCapabilities(true),
        this.samplingHandler != null ? new McpSchema.ClientCapabilities.Sampling() : null,
        this.elicitationHandler != null ? new McpSchema.ClientCapabilities.Elicitation() : null);
  }

  // --- builder -------------------------------------------------------------

  /** Fluent configuration, mirroring {@code VertxMcpServer.Builder}. */
  public static final class Builder {

    private final Vertx vertx;

    private McpClientTransport transport;

    private McpSchema.Implementation clientInfo = new McpSchema.Implementation("vertx-mcp-client", "0.1.0");

    private McpSchema.ClientCapabilities capabilities;

    private Duration requestTimeout = Duration.ofSeconds(30);

    private JsonRpcCodec codec;

    private String sessionId;

    private final List<McpSchema.Root> roots = new ArrayList<>();

    private McpClientFeatures.SamplingHandler samplingHandler;

    private McpClientFeatures.ElicitationHandler elicitationHandler;

    private McpClientFeatures.LoggingHandler loggingHandler;

    private McpClientFeatures.ProgressHandler progressHandler;

    private McpClientFeatures.ListChangedHandler toolsChangeHandler;

    private McpClientFeatures.ListChangedHandler resourcesChangeHandler;

    private McpClientFeatures.ListChangedHandler promptsChangeHandler;

    private McpClientFeatures.ResourceUpdatedHandler resourceUpdatedHandler;

    private Builder(Vertx vertx) {
      this.vertx = vertx;
    }

    public Builder transport(McpClientTransport transport) {
      this.transport = transport;
      return this;
    }

    public Builder clientInfo(String name, String version) {
      this.clientInfo = new McpSchema.Implementation(name, version);
      return this;
    }

    public Builder clientInfo(McpSchema.Implementation clientInfo) {
      this.clientInfo = clientInfo;
      return this;
    }

    /**
     * Override the capabilities advertised at initialize. Derived from the handlers and
     * roots you register otherwise.
     */
    public Builder capabilities(McpSchema.ClientCapabilities capabilities) {
      this.capabilities = capabilities;
      return this;
    }

    /** How long to wait for the server's answer to any single request. */
    public Builder requestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public Builder codec(JsonRpcCodec codec) {
      this.codec = codec;
      return this;
    }

    /** Prefix for generated JSON-RPC request ids. Generated when not set. */
    public Builder sessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    /** Answers {@code sampling/createMessage}. Declares the sampling capability. */
    public Builder sampling(McpClientFeatures.SamplingHandler handler) {
      this.samplingHandler = handler;
      return this;
    }

    /** Answers {@code elicitation/create}. Declares the elicitation capability. */
    public Builder elicitation(McpClientFeatures.ElicitationHandler handler) {
      this.elicitationHandler = handler;
      return this;
    }

    /** The roots answered to {@code roots/list}. */
    public Builder roots(List<McpSchema.Root> roots) {
      this.roots.clear();
      this.roots.addAll(roots);
      return this;
    }

    public Builder root(McpSchema.Root root) {
      this.roots.add(root);
      return this;
    }

    public Builder toolsChangeHandler(McpClientFeatures.ListChangedHandler handler) {
      this.toolsChangeHandler = handler;
      return this;
    }

    public Builder resourcesChangeHandler(McpClientFeatures.ListChangedHandler handler) {
      this.resourcesChangeHandler = handler;
      return this;
    }

    public Builder promptsChangeHandler(McpClientFeatures.ListChangedHandler handler) {
      this.promptsChangeHandler = handler;
      return this;
    }

    /** Called for a subscribed resource the server says has changed. */
    public Builder resourceUpdatedHandler(McpClientFeatures.ResourceUpdatedHandler handler) {
      this.resourceUpdatedHandler = handler;
      return this;
    }

    public Builder loggingHandler(McpClientFeatures.LoggingHandler handler) {
      this.loggingHandler = handler;
      return this;
    }

    public Builder progressHandler(McpClientFeatures.ProgressHandler handler) {
      this.progressHandler = handler;
      return this;
    }

    public VertxMcpClient build() {
      if (this.transport == null) {
        throw new IllegalArgumentException("A transport is required");
      }
      VertxMcpClient client = new VertxMcpClient(this);
      logger.debug("Built MCP client {} with capabilities {}", client.clientInfo(), client.clientCapabilities());
      return client;
    }
  }
}
