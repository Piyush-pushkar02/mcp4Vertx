# Contributing to mcp4vertx

Thanks for wanting to help. This document covers how to build the project, how it is laid
out, and the conventions a patch is expected to follow.

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE), the same licence as the project.

## Before you start

- **Bugs and small fixes** — open a pull request directly. A failing test that demonstrates
  the bug is worth more than a paragraph describing it.
- **New features or public API changes** — open an issue first. mcp4vertx tracks the
  [Model Context Protocol specification](https://modelcontextprotocol.io/specification);
  agreeing on what the spec requires is usually most of the design work, and it is cheaper
  to do that before code exists.
- **Security issues** — do not open an issue. See [SECURITY.md](SECURITY.md).

Everyone taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Building

You need **JDK 17 or later** and **Maven 3.9+**. There are no other prerequisites; the
conformance suite downloads the official SDK and an embedded Jetty from Maven Central.

```bash
mvn verify
```

That compiles all four modules, runs the full test suite, and checks that every source file
carries its licence header. This is the same command CI runs, so if it passes locally it
will almost certainly pass in CI. CI additionally runs it on both JDK 17 and JDK 21.

Other useful invocations:

```bash
mvn install                       # also install the jars into ~/.m2 for local use
mvn -q verify                     # quiet; only failures and test output
mvn verify -DskipTests            # compile and licence-check only
mvn -pl mcp4vertx-server -am verify   # one module and what it depends on
```

`mvn verify` takes a couple of minutes, mostly in the conformance suite, which starts real
HTTP servers on ephemeral ports.

### Running a single test

Surefire's `-Dtest` selector takes a class, a method, or a pattern. Combine it with `-pl` to
skip the modules you are not touching:

```bash
# one class
mvn -pl mcp4vertx-client test -Dtest=SseParserTest

# one method
mvn -pl mcp4vertx-client test -Dtest='SseParserTest#parsesMultiLineData'

# a pattern across a module
mvn -pl mcp4vertx-examples test -Dtest='*ConformanceTest'

# one test, whole reactor, without failing on modules that match nothing
mvn test -Dtest=SseParserTest -DfailIfNoSpecifiedTests=false
```

To see more log output from a test run, raise the SLF4J simple-logger level. The root POM
pins it to `info` for the whole suite:

```bash
mvn -pl mcp4vertx-core test -Dtest=McpStreamableServerSessionTest \
    -DargLine=-Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

### Licence headers

Every `.java` file under `src/main/java` and `src/test/java` must start with the Apache 2.0
header. `mvn verify` fails if one is missing or altered. You never have to write it by hand:

```bash
mvn license:format     # add or repair the header on every file that needs it
mvn license:check      # what CI checks
```

Run `license:format` after adding a new source file and the check will pass.

## How the code is laid out

Four modules. The first three are published to Maven Central; the fourth is not.

| Module | What lives there |
|---|---|
| `mcp4vertx-core` | The protocol layer with no I/O in it: JSON-RPC codec, sessions, the transport SPI, the event store |
| `mcp4vertx-server` | `VertxMcpServer`, the feature registry, and the four server transports |
| `mcp4vertx-client` | `VertxMcpClient` and the three client transports |
| `mcp4vertx-examples` | Runnable examples plus the cross-SDK conformance suite |

Some landmarks worth knowing before you go looking:

- **Wire types are not ours.** Everything under `io.modelcontextprotocol.spec.McpSchema` comes
  from the official SDK's `mcp-core` artifact and is reused verbatim, with `reactor-core`
  excluded in the root POM. Protocol revisions churn those DTOs and that stays upstream's
  problem. Do not add a parallel DTO hierarchy.
- **`AbstractMcpSession`** (`mcp4vertx-core`, `session` package) is the heart of the project.
  It captures a Vert.x `Context` in its constructor and every entry point hops onto that
  context before touching state. `McpClientSession`, `McpServerSession` and
  `McpStreamableServerSession` build on it.
- **`Contexts`** (`mcp4vertx-core`, `internal` package) is the one place that decides which
  context work runs on. If you find yourself writing `vertx.runOnContext` in a transport,
  the helper you want is probably already there.
- **Transports implement an SPI**, they do not own the protocol. Server transports expose a
  vertx-web `Router` so an application can mount MCP as a sub-router alongside its own
  routes; they do not create the `HttpServer`.
- **`EventStore`** (`mcp4vertx-core`, `spec` package) is a four-method SPI behind
  `Last-Event-ID` replay. `InMemoryEventStore` is the default; a clustered deployment
  supplies its own.

[`docs/threading.md`](docs/threading.md) explains the context-pinning model that the whole
design follows. Read it before changing anything in `session` or in a transport — it is the
reason the library exists, and it is easy to break by accident.

## Code style

The style below is what the existing sources already do. Match the file you are editing.

- **Two-space indent**, spaces only, never tabs. Continuation lines indent by four.
- **120-column limit.** A handful of existing lines exceed it; do not use them as licence.
- **`catch` and `else` start their own line**, they do not trail the closing brace:

  ```java
  try {
    return action.get();
  }
  catch (Throwable t) {
    return Future.failedFuture(t);
  }
  ```

- **A blank line between field declarations.** Fields are read one at a time, so they get
  their own visual line each:

  ```java
  protected final Vertx vertx;

  protected final Context context;

  protected final JsonRpcCodec codec;
  ```

- **Imports** are grouped `java.*`, then third-party (`com.*`, `org.*`), then `io.*`, with a
  blank line between groups. No wildcard imports, no static wildcard imports.
- **Javadoc on everything public.** Public types and public methods carry javadoc. The
  release build runs javadoc with `-Xdoclint:all,-missing`, so a broken `{@link}` or
  malformed HTML fails the build even though a missing `@param` does not. Write `@param` and
  `@return` where the meaning is not already obvious from the prose; the check will not
  force you, but a reader will thank you.
- **Comments explain WHY, not what.** The code already says what it does. A comment earns
  its place by recording the reason a non-obvious choice was made — a spec requirement, a
  race that has to be avoided, an upstream bug being worked around. The existing comments in
  the POMs and in the transports are the model here.
- **Files end with a newline**, no trailing whitespace. [`.editorconfig`](.editorconfig)
  covers the whitespace rules automatically in any editor that reads it.

## Tests

**New behaviour needs a test.** A pull request that changes what the library does and adds no
test will be asked for one. A bug fix should come with a test that fails before the fix.

Tests use JUnit 5, AssertJ assertions and `vertx-junit5`. Some conventions:

- Test names describe the behaviour, not the method: `replaysEventsAfterLastEventId`, not
  `testReplay`.
- Prefer a real transport over a mock. Most of the suite starts an actual HTTP server on an
  ephemeral port; that is deliberate, because the bugs worth catching in this project live in
  the interaction between the session and the wire, not inside either one.
- Never hard-code a port. Listen on `0` and ask the server which port it got.
- Anything touching protocol behaviour that the official SDK also implements belongs in the
  conformance suite in `mcp4vertx-examples`, which runs both directions: the official client
  against our server, and our client against the official servlet server in Jetty.
  See [`docs/conformance.md`](docs/conformance.md).

## Pull requests

- Branch from `main`, and keep one logical change per pull request.
- Write a commit message that says why, not just what. The subject line is imperative mood
  and under about 72 characters.
- Add an entry to the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md) for anything a
  user would notice.
- Update `docs/` and `README.md` when you change public API.
- Make sure `mvn verify` passes before asking for review.

Maintainers may ask for changes on design grounds even when the code is correct — usually
because a change would compromise the context-affinity guarantee or would widen the public
API more than the problem requires. That is not a comment on the work; it is the cost of a
library whose whole value proposition is a narrow, predictable threading model.
