# Development

For repository ownership and record flow, see the
[architecture overview](.agents/skills/kafka-connect-arangodb-development/references/architecture.md).
For Maven commands, disposable test infrastructure, the four CircleCI test jobs,
their matrices, and failure diagnostics, see the
[build and test guide](.agents/skills/kafka-connect-arangodb-development/references/testing.md).
These guides apply to human contributors as well as coding agents.

Agent entry point: [AGENTS.md](AGENTS.md). The
[change guide](.agents/skills/kafka-connect-arangodb-development/references/changes.md)
covers configuration, record handling, lifecycle, and packaging boundaries.

## Check dependency updates

```sh
mvn versions:display-dependency-updates
mvn versions:display-plugin-updates
```
