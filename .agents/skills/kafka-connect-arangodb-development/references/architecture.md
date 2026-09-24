# Architecture

## Runtime ownership

Production paths in this table are relative to
`src/main/java/com/arangodb/kafka/`.

| Component | Responsibility |
| --- | --- |
| `ArangoSinkConnector` | Parse connector configuration, own `HostListMonitor`, and generate task configurations with rotated endpoint lists. |
| `HostListMonitor` | Optionally discover cluster endpoints, periodically shuffle endpoints, and request task reconfiguration. Own a scheduler and, when discovery is enabled, a separate driver client. |
| `ArangoSinkTask` | Own a task's collection/client and writer, check connectivity and write permission at startup, delegate `put`, and shut down the task client. |
| `config/ArangoSinkConfig` | Define the public `ConfigDef`, validate configuration, map protocol/content type and TLS settings to driver clients, and construct write options. |
| `conversion/RecordConverter` | Convert Connect values to Jackson objects and assign document identity. |
| `conversion/KeyConverter` | Map document/record keys and generate the fallback key. |
| `ArangoWriter` | Form ordered batches, execute synchronous document operations, inspect per-document results, and implement data-error and retry policy. |
| `TransientException` | Connector-internal transient failure classification; distinct from Kafka's `RetriableException`. |

## Record flow

```text
Kafka bytes
  -> worker key/value converters -> SinkRecord
  -> ArangoSinkTask.put -> ArangoWriter
  -> contiguous insert/delete batches
  -> RecordConverter / KeyConverter
  -> ArangoCollection.insertDocuments / deleteDocuments
  -> ordered document/error results -> success, reporting, retry, or failure
```

Worker converters deserialize the Kafka payload before the sink sees it. The
connector's converters use Kafka's `JsonConverter` internally to turn Connect
schemas/values into schema-free JSON; they do not parse arbitrary Kafka bytes.
All subscribed topics feed the one database/collection configured for the
connector. `createCollection()` obtains a handle; production startup does not
create a missing collection. Test fixtures create their own collections.

Document identity prefers a non-null value `_key`, then the record key, then
`topic-partition-offset`. Supported explicit keys are strings or integral
numbers. A non-null record key with a null value takes the delete path; deletes
must be enabled. A null key and null value instead takes the insert path and
becomes an object with the generated key.

`ArangoWriter` splits records into contiguous same-operation batches bounded by
`batch.size`. It correlates `getDocumentsAndErrors()` with input records by index.
`currentOffset` is an index into the collection passed to `put`, not a Kafka
partition offset. It and the remaining retry budget survive a retriable failure
so a subsequent delivery of that collection resumes at the failed batch. The
failed batch can already have partial database effects. The retry budget applies
per batch: it resets after each completed batch, and progress resets when the
whole collection completes. This is not an exactly-once or transactional
delivery mechanism.

The task does not implement custom `flush`, `preCommit`, or offset storage.
Writes complete synchronously or throw back to Connect; adding asynchronous
buffering would change that contract.

## Control flow and resource ownership

The connector's monitor requests reconfiguration when a non-empty discovered
host set changes, and periodically for connection rebalancing even when discovery
is disabled. Task configurations rotate the ordered list to distribute starting
endpoints. The monitor and task clients are separate and have separate shutdown
paths. Endpoint access and mutation span Connect callbacks and a scheduled
thread; `getEndpoints()` currently returns the mutable list, not a snapshot.

## Build and test boundaries

| Path | Role |
| --- | --- |
| `pom.xml` | One artifact; dependency scopes, generated version source, assembly, test phases, and deployment-selection profiles. |
| `src/main/assembly/jar-with-dependencies.xml` | Runtime dependency assembly with merged service descriptors; worker-provided Jackson core artifacts are excluded. |
| `src/main/java/com/arangodb/kafka/PackageVersion.java.in` | Input for the generated version class under `target/generated-sources/replacer/`. |
| `src/test/java/com/arangodb/kafka/` | Unit tests, integration tests, targets, and test utilities. |
| `src/test/java/deployment/` | External services and the `KafkaConnectDeployment` service-loading boundary. |
| `src/connect-standalone/`, `src/connect-distributed/` | Alternative **test** sources/resources, not production modules; select one deployment provider via Maven properties. |
| `docker/`, `bin/startProxy.sh`, `.circleci/config.yml` | Integration infrastructure and the CI job definitions. |
| `demo/` | Separate demonstration environment, not the CI test harness. |
