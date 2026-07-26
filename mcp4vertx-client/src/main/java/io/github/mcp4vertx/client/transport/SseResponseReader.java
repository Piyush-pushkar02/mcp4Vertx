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

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.github.mcp4vertx.internal.Contexts;

/**
 * Reads a {@code text/event-stream} response body off an {@link HttpClientResponse} and
 * hands each event to a consumer the moment it completes.
 *
 * <p>
 * This is what makes the interesting MCP responses work. A {@code tools/call} may answer
 * with a stream and emit progress notifications, and even a
 * {@code sampling/createMessage} request back to us, long before the final result -- all
 * of which must reach the session as they arrive, not when the response finally ends.
 * {@link HttpClientResponse} is a {@code ReadStream<Buffer>}, so the chunks are fed to
 * {@link SseParser} straight off the socket.
 *
 * <p>
 * Reading is demand-driven rather than flowing: the response is paused up front and
 * exactly one chunk is fetched at a time, the next only once every event the previous
 * chunk completed has been taken by the consumer. A consumer that cannot keep up
 * therefore stops the socket -- the read demand stays at zero, the TCP window closes and
 * the server is throttled -- instead of letting Vert.x queue chunks in memory without
 * bound. The cost when the consumer is fast, which is the normal case, is one extra
 * {@code fetch} per chunk.
 *
 * <p>
 * Not thread-safe, and does not need to be. The transport issues its requests from its own
 * Vert.x context, so the response is read on that context and events are delivered back to
 * it; every field below is therefore only ever touched from one thread.
 */
final class SseResponseReader {

  private static final Logger logger = LoggerFactory.getLogger(SseResponseReader.class);

  private final Context context;

  private final HttpClientResponse response;

  private final Consumer<SseEvent> handler;

  private final SseParser parser;

  private final Promise<Void> completion = Promise.promise();

  /** Events handed to the consumer but not yet taken; non-zero means do not fetch. */
  private int inFlight;

  /** True while inside {@link #onChunk}, where the fetch is issued by the caller instead. */
  private boolean dispatching;

  private boolean finished;

  SseResponseReader(Context context, HttpClientResponse response, Consumer<SseEvent> handler) {
    this.context = context;
    this.response = response;
    this.handler = handler;
    this.parser = new SseParser(this::onEvent);
  }

  /** Last SSE {@code id:} observed on this stream, for {@code Last-Event-ID} resumption. */
  String lastEventId() {
    return this.parser.lastEventId();
  }

  /** The server's suggested reconnection delay, or {@code null} if it never sent one. */
  Long retryMillis() {
    return this.parser.retryMillis();
  }

  /**
   * Starts reading.
   *
   * <p>
   * The returned future completes when the stream ends and fails if it breaks. Either way
   * nothing is dispatched after it settles, so a caller may treat it as the end of the
   * stream's life.
   */
  Future<Void> start() {
    this.response.pause();
    this.response.handler(this::onChunk);
    this.response.endHandler(ignored -> onEnd());
    this.response.exceptionHandler(this::fail);
    this.response.fetch(1);
    return this.completion.future();
  }

  // --- reading -------------------------------------------------------------

  private void onChunk(Buffer chunk) {
    this.dispatching = true;
    try {
      this.parser.write(chunk);
    }
    catch (RuntimeException e) {
      // A malformed stream -- a line longer than the parser's cap -- is not recoverable,
      // and there is no point letting the server keep sending.
      this.dispatching = false;
      this.response.request().reset();
      fail(e);
      return;
    }
    this.dispatching = false;
    fetchMore();
  }

  private void onEnd() {
    if (this.finished) {
      return;
    }
    // Marked finished before the flush below, which can dispatch one last event and must
    // not go on to ask a stream that is already over for another chunk.
    this.finished = true;
    try {
      // Flushes a trailing event the server did not terminate with a blank line, which is
      // common enough that dropping it would strand the caller's request.
      this.parser.end();
    }
    catch (RuntimeException e) {
      logger.debug("Failed to flush the trailing SSE event", e);
    }
    this.completion.tryComplete();
  }

  private void fail(Throwable cause) {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.completion.tryFail(cause);
  }

  // --- backpressure --------------------------------------------------------

  private void onEvent(SseEvent event) {
    this.inFlight++;
    // Inline whenever the consumer lives on the reading context, which is the normal case.
    // When it does not, the socket stays unfetched until the consumer has actually taken
    // the event -- that is where the backpressure bites.
    Contexts.run(this.context, () -> {
      try {
        this.handler.accept(event);
      }
      finally {
        onDelivered();
      }
    });
  }

  private void onDelivered() {
    this.inFlight--;
    if (!this.dispatching) {
      fetchMore();
    }
  }

  private void fetchMore() {
    if (!this.finished && this.inFlight == 0) {
      this.response.fetch(1);
    }
  }
}
