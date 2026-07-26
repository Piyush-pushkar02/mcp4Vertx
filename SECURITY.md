# Security Policy

## Supported versions

mcp4vertx is pre-1.0. Until 1.0, security fixes are made on the latest released minor
version only; there are no long-term support branches, and there is no backporting to
earlier 0.x lines.

| Version | Supported |
|---|---|
| 0.1.x   | Yes — current release line |
| < 0.1   | No — no such release exists |

If you are running an older 0.x once later ones exist, the fix will be to upgrade. Because
the project is pre-1.0 a patch release may contain a small breaking change if that is what a
correct fix requires; when it does, the [CHANGELOG](CHANGELOG.md) will say so explicitly.

Note that mcp4vertx sits on top of Eclipse Vert.x, Jackson and the official MCP SDK's
`mcp-core`. A vulnerability in one of those is best reported to that project directly. If a
mcp4vertx release pins a version of one of them that is known to be vulnerable, that is our
problem and we do want to hear about it.

## Reporting a vulnerability

**Please do not open a public issue, pull request or discussion for a security problem.**

Report privately through GitHub's private vulnerability reporting:

**<https://github.com/mcp4vertx/mcp4vertx/security/advisories/new>**

That form is visible only to the maintainers. It also gives us a private fork to develop and
review the fix in, and it is how the eventual advisory and CVE get published.

If you cannot use that form for any reason, say so in a GitHub issue *without any detail
about the vulnerability itself* and a maintainer will arrange another channel.

### What to include

The more of this you can supply, the faster the fix:

- The version of mcp4vertx, and of Vert.x and the JDK you are running.
- Which module and transport are involved (`StreamableHttpServerTransport`,
  `SseServerTransport`, `StatelessHttpServerTransport`, `StdioServerTransport`, or a client
  transport).
- What an attacker can achieve, and what access they need to start with — for example
  whether an unauthenticated remote caller can reach it, or whether it requires an already
  established session.
- A reproduction. A failing test against this repository, or a minimal server plus the
  requests that trigger the problem, is ideal.
- Whether you have already disclosed this anywhere else.

Areas that are especially worth a close look: session-identifier handling and the
`Last-Event-ID` replay path in the streamable HTTP transport, `Origin` and header validation
on the HTTP transports, resource-URI template expansion, and JSON parsing limits.

### What to expect

| | |
|---|---|
| Acknowledgement of your report | within **3 business days** |
| Initial assessment, with a severity and whether we accept it | within **10 business days** |
| Fix released, for a confirmed vulnerability | target **90 days**, sooner for high severity |

We will keep you updated as the assessment progresses, and we will tell you if any of these
are going to slip rather than going quiet.

mcp4vertx is a volunteer-maintained project. These are goals we intend to meet, not a
contractual service level.

### Disclosure

We follow coordinated disclosure. We ask that you give us the 90-day window above before
disclosing publicly, and we will work with you if you have a deadline of your own. When the
fix ships we publish a GitHub security advisory, request a CVE where one is warranted, and
note it in the [CHANGELOG](CHANGELOG.md).

We will credit you in the advisory by whatever name or handle you prefer, or leave you
anonymous if you would rather. There is no bug-bounty programme and no payment.

### Safe harbour

We will not pursue or support legal action against anyone who makes a good-faith effort to
follow this policy: research on your own systems, no access to or modification of other
people's data, no service degradation, and private reporting before disclosure. If you are
unsure whether something is in scope, ask first through the form above.
