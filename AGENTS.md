# Kafka Connect ArangoDB: agent guide

This repository is a single-module Maven project implementing an ArangoDB sink
for Kafka Connect. It is not a Kafka source connector or the ArangoDB Java driver.

For code changes, refactoring, dependency updates, or tests, use the
[development skill](.agents/skills/kafka-connect-arangodb-development/SKILL.md).
Load only the references relevant to the task. Agents without skill discovery
can read the same files directly.

## Build and test

Use Maven 3.9+ (`mvn`, no wrapper). `mvn test` runs the unit tests without
external services. Integration tests (`*IT`) run with `mvn integration-test`
against the Docker harness described in the
[testing guide](.agents/skills/kafka-connect-arangodb-development/references/testing.md);
do not use `mvn verify`, which also triggers GPG signing.

## Working boundaries

- Use source, `pom.xml`, and `.circleci/config.yml` to establish current behavior
  and build configuration. They take precedence over stale descriptions here;
  correct relevant documentation drift. Existing behavior is not proof that a
  reported bug is intended.
- Keep Java sources compatible with the POM's source/target 8 settings, including
  tests. These settings do not prevent accidental use of newer JDK APIs. Run
  builds on the JDKs specified in the testing guide; do not infer Java 8 runtime
  support from the connector's bytecode target.
- Treat configuration names/defaults, document identity, write ordering, and
  failure/retry behavior as compatibility boundaries. Change them deliberately,
  with regression coverage, rather than as incidental cleanup.
- Match nearby code and tests. Avoid unrelated formatting, version bumps, and
  dependency changes. Edit source inputs, not `target/`, generated
  `PackageVersion.java`, or `.flattened-pom.xml`. The bundled datagen connector
  under `demo/data/connectors/` is third-party content, not connector source.
- Use only disposable infrastructure for integration tests: fixtures recreate
  collections and Kafka topics, and the Docker scripts use fixed resource names
  and mount the Docker socket. Do not use release/deploy commands for validation.

Report the behavior changed, checks actually run and their results, and relevant
untested variants. Distinguish environment failures from regressions; a build
with skipped or unselected tests is not evidence that the behavior passed.
