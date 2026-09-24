---
name: kafka-connect-arangodb-development
description: >-
  Maintain and extend the kafka-connect-arangodb repository. Use for connector
  features, bug fixes, refactoring, configuration and dependency updates, record
  conversion, batching, retries, host discovery, packaging, or selecting and
  diagnosing tests that correspond to CircleCI. Not a guide to configuring the
  released connector in an application.
---

# Kafka Connect ArangoDB development

Apply the repository's [AGENTS.md](../../../AGENTS.md). Run commands from the
repository root. Read the relevant reference sections, not the entire bundle.

## Locate the change

Identify the observable contract and the layer that owns it. Trace a representative
record or lifecycle operation through the affected code and a neighboring test.
For a bug, establish a reproducer; for a feature, identify configuration and
compatibility effects; for a refactor, identify behavior that must stay fixed.

| Task | Read when needed |
| --- | --- |
| Locate ownership or trace delivery | [Architecture](references/architecture.md) |
| Change options, defaults, validation, or client construction | [Configuration](references/changes.md#configuration) |
| Change conversion, identity, writes, or error handling | [Records and writes](references/changes.md#records-and-writes), [errors and retries](references/changes.md#errors-and-retries) |
| Change startup, shutdown, host discovery, or rebalancing | [Lifecycle and concurrency](references/changes.md#lifecycle-and-concurrency) |
| Change dependencies, generated code, or plugin packaging | [Build and packaging](references/changes.md#build-and-packaging) |
| Add tests, select CI coverage, or diagnose a failure | [Testing](references/testing.md) |

## Implement and validate

Choose the smallest coherent change at the owning layer. Add reproducing tests
for fixes and coverage for new behavior; for refactors, retain or extend checks
of observable equivalence. Reuse the existing unit-test and `@KafkaTest` fixtures
rather than introducing a second harness.

Start with the focused check from the testing guide, then select the CI job and
variants that exercise the changed boundary. Distributed artifact changes need
fresh worker deployment, not just another Maven invocation. Documentation-only
changes need path, command, and diff checks, not database startup.

Update affected configuration descriptions, examples, and developer guidance.
Record user-visible changes in `ChangeLog.md` under `Unreleased`; do not rewrite
released history. Review the final diff for incidental behavior changes and
report validation as specified in `AGENTS.md`.
