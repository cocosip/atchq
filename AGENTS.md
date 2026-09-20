# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## Project Overview

LatchQ is a Java 21 persistent queue library built on OpenHFT Chronicle Queue. It supports
multi-threaded concurrent production, multi-threaded concurrent batch consumption, out-of-order
commits with interval merging, gap detection/forcing, durable consumption checkpoints, automatic
cleanup of consumed cycle files and index-range export. It is a Java port of the semantics of
`SharpAbp.Abp.Faster` (the .NET/FASTER reference implementation), with the storage engine swapped
to Chronicle Queue.

- Module layout: `latchq-parent` (root pom) + `latchq-core`; `latchq-spring-boot-starter`,
  `samples/*` and `benchmarks` are added by later milestones.
- Authoritative documents live in `docs/`: `latchq-design.md` (design, frozen v0.3),
  `latchq-progress.md` (milestone plan and progress), `spike-notes.md` (verified Chronicle
  behaviour). Documentation is written in Simplified Chinese.

## Build and Test

- Always build through the committed Maven Wrapper; never rely on a globally installed Maven:

  ```bash
  ./mvnw test            # unit + integration tests
  ./mvnw verify          # tests + jacoco + spotless checks
  ./mvnw spotless:apply  # format code before committing
  ```

- JDK 21 is required. Chronicle needs JVM opens on JDK 21; the flag set is already pinned in the
  root pom (`maven-surefire-plugin` `argLine`) and documented in `docs/spike-notes.md`. Any
  spawned JVM (samples, benchmarks, crash-test child processes) must carry the same flags.
- Test data must be written under `target/test-data/` (wiped by `mvn clean`), never under
  `src/`. Do not use JUnit `@TempDir`: Chronicle releases `.cq4` file handles asynchronously on
  Windows, which makes temp-directory cleanup flaky.

## Code Style

- All source code comments, javadoc and commit messages are written in English. Documentation
  under `docs/` is written in Simplified Chinese. Never mix languages within a single file
  category.
- Formatting is enforced by Spotless (google-java-format, AOSP style, 4-space indent). Run
  `./mvnw spotless:apply` before committing.
- Chronicle types (`ExcerptAppender`, `ExcerptTailer`, `Wire`, ...) must not leak into the public
  API; keep them inside `io.github.cocosip.latchq.internal`. The one sanctioned exception is the
  `builderCustomizer` hook in `LatchQueueConfiguration`, which exists precisely to hand advanced
  users the native `SingleChronicleQueueBuilder` (see the design document, chapter 8).
- Known behavioural constraints discovered in M0 (see `docs/spike-notes.md`) must be respected:
  scan threads use unnamed tailers only; message positions are chained via read-ahead stamped
  real next indexes (with provisional in-cycle tail delivery plus empty-gap corrections);
  empty queues report `firstIndex() == Long.MAX_VALUE` at the Chronicle layer and `-1` at the
  LatchQ layer.

## Commit Message Rules

Commits follow Conventional Commits with Gitmoji. All commit content is in English. Never use
milestone names (M0, M1, ...) as the commit subject; describe the change itself.

Format:

```
<emoji> <type>(optional-scope): <description>
[blank line]
[optional body with "- " bullet points]
[blank line]
[optional footers]
```

Emoji + type mapping (pick the closest match):

- `✨ feat` - new feature
- `🐛 fix` - bug fix
- `🔧 chore` - maintenance, tooling
- `🏗️ build` - build system or dependency changes
- `👷 ci` - CI configuration
- `📝 docs` - documentation only
- `♻️ refactor` - neither fixes a bug nor adds a feature
- `⚡️ perf` - performance improvement
- `💄 style` - formatting, no logic change
- `✅ test` - adding or correcting tests
- `🌐 i18n` - internationalization
- `⏪️ revert` - revert

Rules:

- Subject: imperative mood, no trailing period, max 100 characters including the emoji.
- Scope (lowercase, derived from the paths touched, e.g. `core`, `starter`, `docs`, `build`) is
  mandatory when the diff clearly targets one module or area; omit it only when no single scope
  fits.
- Body: optional, one blank line after the subject, `- ` bullets, each line <= 100 characters,
  verifiable facts only.
- Footers: `Token: value` (e.g. `Fixes: #12`, `BREAKING CHANGE: ...`), each line <= 100
  characters.
- Dependency bumps: list direct dependencies as `- library: old -> new` bullets.
- Unrelated concerns belong in separate commits, each following the format above.
