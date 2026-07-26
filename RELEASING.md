# Releasing mcp4vertx

This is the maintainer's runbook. Part 1 is one-time setup and is only done once for the
lifetime of the project. Part 2 is what you do for each release.

Publishing goes through the **Central Publisher Portal** at <https://central.sonatype.com>.
The legacy OSSRH staging API at `oss.sonatype.org` is gone; anything you read that mentions
`nexus-staging-maven-plugin` or an `ossrh` server id is out of date. This project uses
`org.sonatype.central:central-publishing-maven-plugin`, configured in the `release` profile
of the root `pom.xml`.

---

## Before you publish anything: coordinates are permanent

Read this once, carefully, because it is the only decision in this document that cannot be
undone.

**A published Maven Central coordinate is immutable and permanent.** Once
`io.github.mcp4vertx:mcp4vertx-core:0.1.0` exists on Central it can never be modified,
replaced or deleted. Central does not support unpublishing. A bad release is fixed by
publishing a *new version*, not by removing the old one.

**The groupId is a long-term commitment.** This project uses `io.github.mcp4vertx`, which
Central grants on the basis of ownership of the `mcp4vertx` GitHub organisation. That is the
cheapest namespace to obtain and it requires no domain name.

If the project later wants a domain-based groupId — say `io.mcp4vertx` or `dev.mcp4vertx` —
that is **not a rename**. It requires:

1. owning the domain,
2. registering a *new* namespace on the Portal and verifying it by DNS TXT record,
3. publishing the artifacts again under the new groupId, as a new coordinate line,
4. keeping the old `io.github.mcp4vertx` artifacts published forever, because they cannot be
   removed, and telling users which line is current.

Consumers do not get an automatic upgrade path across a groupId change; every downstream
`pom.xml` has to be edited by hand. So: either accept `io.github.mcp4vertx` for good, or
sort the domain out **before** the first release. Do not publish 0.1.0 intending to "fix the
groupId later".

---

## Part 1 — one-time setup

### 1.1 Create the GitHub organisation

The namespace `io.github.mcp4vertx` is verified against the GitHub account named
`mcp4vertx`, so that account must exist and you must control it.

1. Go to <https://github.com/organizations/plan> and create a **free organisation** named
   exactly `mcp4vertx`. The name must match the namespace exactly — Central checks
   `github.com/mcp4vertx`, not a personal account that happens to own the repository.
2. Create the repository `mcp4vertx` inside it and push this codebase to `main`.
3. In **Settings → General**, enable **Issues** and **Discussions** (the issue template
   config links to Discussions).
4. In **Settings → Code security**, enable **Private vulnerability reporting**. `SECURITY.md`
   directs reporters to that form, and it does not exist until you turn it on.
5. In **Settings → Actions → General**, confirm workflows are allowed to run, and that
   **Workflow permissions** allow the `GITHUB_TOKEN` to be used as configured (the release
   workflow requests `contents: write` explicitly, so the repository default can stay
   read-only).

### 1.2 Register the namespace on the Central Portal

1. Sign in at <https://central.sonatype.com> — use **Sign in with GitHub** and authorise it
   for the `mcp4vertx` organisation, which makes the next step much smoother.
2. Go to **View Namespaces** → **Add Namespace**.
3. Enter `io.github.mcp4vertx` and submit.
4. The Portal marks it *unverified* and shows a **verification code** — a random string such
   as `a1b2c3d4e5`.
5. **Create a public repository whose name is exactly that verification code**, inside the
   `mcp4vertx` organisation: `https://github.com/mcp4vertx/a1b2c3d4e5`. It can be completely
   empty; the Portal only checks that it exists under that account.
6. Back on the Portal, click **Verify Namespace**. Verification is usually immediate.
7. Once the namespace shows as verified, the temporary repository has done its job and can
   be deleted.

### 1.3 Generate and publish a GPG key

Central requires every artifact to carry a detached signature from a key it can find on a
public keyserver.

```bash
# 4096-bit RSA, no expiry problems mid-release. Use a real, monitored email address.
gpg --full-generate-key
#   (1) RSA and RSA
#   4096
#   0 = does not expire  (or 2y, but then diarise the renewal)
#   Real name:    mcp4vertx release signing
#   Email:        <the maintainer address>
#   Passphrase:   generate a strong one and store it in a password manager
```

Find the key id:

```bash
gpg --list-secret-keys --keyid-format=long
# sec   rsa4096/AB12CD34EF567890 2026-01-01 [SC]
#       ^^^^^^^^^^^^^^^^^^^^^^^^ the long key id is AB12CD34EF567890
```

Publish the **public** key. Central looks it up on the public keyserver network, and
propagation is not instant — do this well before your first release, not during it:

```bash
KEY=AB12CD34EF567890
gpg --keyserver keyserver.ubuntu.com --send-keys "$KEY"
gpg --keyserver keys.openpgp.org     --send-keys "$KEY"
gpg --keyserver pgp.mit.edu          --send-keys "$KEY"

# Confirm it is really there before relying on it:
gpg --keyserver keyserver.ubuntu.com --recv-keys "$KEY"
```

Export the **private** key in ASCII armour — this is what GitHub Actions imports:

```bash
gpg --armor --export-secret-keys "$KEY" > /tmp/mcp4vertx-signing-key.asc
```

Back the key and its passphrase up somewhere durable and offline. Losing it does not
invalidate anything already published, but you will have to generate, publish and re-verify
a new key before you can release again. **Delete `/tmp/mcp4vertx-signing-key.asc` once you
have pasted it into the GitHub secret.**

### 1.4 Generate a Central user token

The Portal authenticates uploads with a generated token pair, never with your account
password.

1. On <https://central.sonatype.com>, open the account menu (top right) → **View Account**.
2. Click **Generate User Token**.
3. The Portal shows an XML `<server>` block **once**. Copy both values out of it:
   - the `<username>` value → this is `CENTRAL_TOKEN_USERNAME`
   - the `<password>` value → this is `CENTRAL_TOKEN_PASSWORD`

   They are opaque random strings, not your email and password.
4. If you lose them, generate a new pair; the old pair is revoked.

The `<id>central</id>` in that block is why the root POM sets
`<publishingServerId>central</publishingServerId>` and why the release workflow passes
`server-id: central` to `actions/setup-java`. Keep all three in agreement.

### 1.5 Add the four repository secrets

In the repository: **Settings → Secrets and variables → Actions → New repository secret**.
Add exactly these four names — the release workflow refers to them literally.

| Secret | Value |
|---|---|
| `MAVEN_GPG_PRIVATE_KEY` | The entire contents of the `.asc` file from 1.3, including the `-----BEGIN PGP PRIVATE KEY BLOCK-----` and `-----END ...-----` lines and the trailing newline |
| `MAVEN_GPG_PASSPHRASE` | The passphrase for that key |
| `CENTRAL_TOKEN_USERNAME` | The token `<username>` from 1.4 |
| `CENTRAL_TOKEN_PASSWORD` | The token `<password>` from 1.4 |

Paste the private key as-is. Do not base64-encode it, and do not strip the newlines —
`actions/setup-java` expects the armoured block verbatim, and a mangled key produces a
confusing `gpg: no valid OpenPGP data found` failure at signing time.

### 1.6 (Optional) Set up local publishing

Only needed if you ever want to publish from a workstation instead of CI. Add to
`~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>${env.CENTRAL_TOKEN_USERNAME}</username>
      <password>${env.CENTRAL_TOKEN_PASSWORD}</password>
    </server>
  </servers>
</settings>
```

and export `CENTRAL_TOKEN_USERNAME`, `CENTRAL_TOKEN_PASSWORD` and `MAVEN_GPG_PASSPHRASE` in
your shell. Keeping the values in environment variables rather than in `settings.xml` means
the file is safe to keep in dotfiles. **CI is the preferred route** — it is reproducible and
leaves an audit trail.

---

## Part 2 — cutting a release

The version is taken **from the git tag**. The POMs stay on a `-SNAPSHOT` version in git and
the release workflow rewrites them in its own workspace with
`versions:set -DnewVersion=<tag without the leading v>`. There is no release commit that
sets a non-snapshot version, and nothing writes back to the repository during a release.

### 2.1 Pre-flight

```bash
git switch main
git pull

# The full build: 81 tests plus the licence-header check, exactly as CI runs it.
mvn -q clean install

# The release build, minus signing. Proves sources and javadoc jars really build
# and that doclint is happy, without needing the GPG key.
mvn -q -Prelease clean package -Dgpg.skip=true
ls */target/*-sources.jar */target/*-javadoc.jar
```

You should see a `-sources.jar` and a `-javadoc.jar` for `mcp4vertx-core`,
`mcp4vertx-server` and `mcp4vertx-client`, and **none for `mcp4vertx-examples`** — that
module is deliberately not published.

Also confirm:

- CI is green on `main` for both JDK 17 and JDK 21.
- `README.md` and `docs/` describe what is actually about to ship.
- No dependency is a `-SNAPSHOT`.

### 2.2 Finalise the changelog

The release workflow **reads `CHANGELOG.md` and fails if there is no `## [<version>]`
section**, so this step is not optional.

1. Move everything from `## [Unreleased]` into a new `## [<version>] - <YYYY-MM-DD>`
   section, using today's date in ISO 8601.
2. Leave `## [Unreleased]` in place with `Nothing yet.` under it.
3. Update the link definitions at the bottom of the file:

   ```markdown
   [Unreleased]: https://github.com/mcp4vertx/mcp4vertx/compare/v<version>...HEAD
   [<version>]: https://github.com/mcp4vertx/mcp4vertx/releases/tag/v<version>
   ```

   For a release after the first, also add a `compare/v<previous>...v<version>` link.

For 0.1.0 specifically, the section already exists but is dated `TBD` — replace that with
the real date.

```bash
git add CHANGELOG.md
git commit -m "Prepare 0.1.0 release notes"
git push
```

### 2.3 Tag and push

The tag must be `v` followed by the version. The workflow strips the `v` and refuses
anything ending in `-SNAPSHOT`.

```bash
git tag -a v0.1.0 -m "mcp4vertx 0.1.0"
git push origin v0.1.0
```

Pushing the tag is what starts the release. Nothing before this point is irreversible.

### 2.4 Watch the workflow

Open **Actions → release**. It will:

1. check out the tagged commit and import the GPG key,
2. derive `0.1.0` from `v0.1.0`,
3. rewrite the POM versions across all modules,
4. extract the `## [0.1.0]` section of the changelog into `release-notes.md`,
5. run `mvn -Prelease deploy` — tests, sources jar, javadoc jar, GPG signatures, and upload
   to the Portal,
6. create the GitHub Release from the extracted notes.

If it fails before step 5 finishes, nothing has been published: fix the problem, delete the
tag (`git tag -d v0.1.0 && git push --delete origin v0.1.0`) and start again from 2.3.

### 2.5 Publish from the Portal

The release profile sets `autoPublish=false` on purpose, so the upload lands as a
**validated but unpublished deployment** and a human makes the final call. This is the last
point at which you can still change your mind.

1. Go to <https://central.sonatype.com> → **View Deployments**.
2. Find the deployment. Wait for it to reach **VALIDATED**.
   - **VALIDATED** — all Central's requirements are met; nothing is public yet.
   - **FAILED** — expand it for the reason. Common causes: a signature Central cannot verify
     against a public keyserver (the key has not propagated — see 1.3), or missing POM
     metadata. Nothing has been published; fix and re-tag.
3. Check the file list. It should contain, for each of the three published modules: the jar,
   `-sources.jar`, `-javadoc.jar`, the `.pom`, and an `.asc` signature plus checksums for
   each of those. `mcp4vertx-examples` must not appear at all.
4. Click **Publish**.
5. If something is wrong, click **Drop** instead. A dropped deployment was never public and
   the version number can be reused.

Once you click Publish, the coordinates are permanent.

### 2.6 After publishing

- Artifacts appear on Central within roughly 10–30 minutes:
  <https://repo1.maven.org/maven2/io/github/mcp4vertx/>
- The <https://central.sonatype.com> search index and the Maven Central badge in the README
  can lag by a few hours. This is normal and is not a failed release.
- Verify from a clean machine or an empty local repository:

  ```bash
  mvn dependency:get -Dartifact=io.github.mcp4vertx:mcp4vertx-server:0.1.0
  ```

- Check the GitHub Release was created and its notes read correctly.
- Bump the development version in git so local builds are no longer named after the version
  you just shipped:

  ```bash
  mvn -q versions:set -DnewVersion=0.2.0-SNAPSHOT -DprocessAllModules=true \
      -DgenerateBackupPoms=false
  git commit -am "Begin 0.2.0 development"
  git push
  ```

- Update the install snippets in `README.md` and `docs/` to the version just released.
- Announce it, if you want to: the Vert.x community channels and the MCP community are the
  natural places.

---

## Troubleshooting

**`gpg: signing failed: Inappropriate ioctl for device`**
GPG is trying to prompt for a passphrase on a terminal that does not exist. The release
profile already passes `--pinentry-mode loopback`; make sure `MAVEN_GPG_PASSPHRASE` is
actually set in the environment of the `mvn` process.

**`gpg: no valid OpenPGP data found`**
The `MAVEN_GPG_PRIVATE_KEY` secret is mangled. Re-copy the whole armoured block, including
the BEGIN and END lines, with no base64 wrapping and no lost newlines.

**Portal deployment is FAILED with a signature error**
The public key has not reached the keyservers Central queries, or you published a different
key from the one that signed. Re-send the key (1.3), confirm with `--recv-keys` from a
machine that does not already have it, wait, and re-tag.

**`401 Unauthorized` from the Portal**
`CENTRAL_TOKEN_USERNAME` / `CENTRAL_TOKEN_PASSWORD` are wrong, or a token was regenerated
and the secrets were not updated. They are the generated token pair from 1.4, not your
Sonatype login. Also check `server-id: central` in the workflow still matches
`<publishingServerId>` in the root POM.

**`No '## [x.y.z]' section found in CHANGELOG.md`**
Step 2.2 was skipped, or the heading does not match the tag. The heading must be exactly
`## [0.1.0]` for tag `v0.1.0`.

**Javadoc fails the release build but `mvn install` is fine**
Expected: javadoc only runs under `-Prelease`. It runs with `-Xdoclint:all,-missing`, so
missing `@param`/`@return` is tolerated but a broken `{@link}` or malformed HTML is an
error. Reproduce locally with
`mvn -Prelease clean package -Dgpg.skip=true`.

**A module was published that should not have been**
`mcp4vertx-examples` is excluded by `maven.deploy.skip`, the central plugin's
`skipPublishing`, and by skipping sources, javadoc and signing — all set in its own
`pom.xml`. If it ever appears in a deployment, stop and Drop the deployment rather than
publishing it.
