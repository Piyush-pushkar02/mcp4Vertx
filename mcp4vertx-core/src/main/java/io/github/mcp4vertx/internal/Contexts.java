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
package io.github.mcp4vertx.internal;

import java.util.function.Supplier;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * Context affinity helpers.
 *
 * <p>
 * Every session in this SDK is pinned to the Vert.x {@link Context} it was created on. All
 * mutable session state -- pending request maps, stream registries, logging levels -- is
 * therefore only ever touched from one thread, so none of it needs locks or concurrent
 * collections, and message ordering within a session is deterministic.
 *
 * <p>
 * This is the thing you cannot get by layering Vert.x transports under a Reactor-based
 * protocol core: {@code Mono} has no notion of a Vert.x context, so any operator that hops
 * schedulers strands the continuation on a thread where {@code Vertx.currentContext()}
 * returns {@code null}.
 */
public final class Contexts {

  private Contexts() {
  }

  /** The context to pin to: the caller's, or a fresh one if called off the event loop. */
  public static Context current(Vertx vertx) {
    Context context = Vertx.currentContext();
    return context != null ? context : vertx.getOrCreateContext();
  }

  public static boolean isOn(Context context) {
    return Vertx.currentContext() == context;
  }

  /** Runs {@code action} on {@code context}, inline if already there. */
  public static <T> Future<T> on(Context context, Supplier<Future<T>> action) {
    if (isOn(context)) {
      try {
        return action.get();
      }
      catch (Throwable t) {
        return Future.failedFuture(t);
      }
    }
    Promise<T> promise = Promise.promise();
    context.runOnContext(v -> {
      try {
        action.get().onComplete(promise);
      }
      catch (Throwable t) {
        promise.tryFail(t);
      }
    });
    return promise.future();
  }

  /** Same as {@link #on} for side-effecting work with no result. */
  public static void run(Context context, Runnable action) {
    if (isOn(context)) {
      action.run();
    }
    else {
      context.runOnContext(v -> action.run());
    }
  }
}
