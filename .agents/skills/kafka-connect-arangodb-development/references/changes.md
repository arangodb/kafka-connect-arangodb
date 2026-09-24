# Change guide

Use the sections that match the affected contract. The behaviors described here
are regression boundaries, not a prohibition on intentionally correcting a bug.

## Configuration

Keep a setting's constant, `CONFIG_DEF` entry, type/default, validator, getter,
consumer, and description aligned in `ArangoSinkConfig`. A registered setting is
not implemented until it reaches the intended client or operation. Add default,
explicit-value, and invalid-value coverage in `ArangoSinkConfigTest`; exercise
observable effects in the relevant integration target.

Distinguish worker settings such as `errors.tolerance` and `errors.deadletterqueue.*`
from this connector's `data.errors.*`. They control different failure boundaries.
Preserve locale-independent enum parsing. For TLS, retain certificate-versus-
truststore validation and check the path as seen inside a distributed worker,
not just on the Maven host. Do not weaken verification to make a test pass or
introduce logging of raw credentials/configuration maps; the existing raw
property-map log in `ArangoSinkConnector.start` is not a precedent.

## Records and writes

Test the boundary affected by the change: schemaful/schemaless conversion, value
`_key` precedence, integral/string keys, generated keys, unsupported value/key
types, and both null-value cases. Do not assume that a JSON-looking Java string
is an object or that every null value is a delete.

Preserve order across insert/delete transitions; globally grouping records by
operation can change the final document. Cover batch boundaries and repeated
keys when changing batching. Keep response cardinality and positional alignment
checks before associating errors with records, including after filtering missing
deletes (ArangoDB error 1202). Missing-document deletes are intentionally
idempotent. Test the selected overwrite mode, object merging, and null handling
when changing write options; stable keys alone do not make all writes idempotent.

## Errors and retries

Keep these outcomes distinct:

- A per-document data error is identified by `DATA_ERROR_NUMS` plus configured
  extra error numbers. Tolerance can fail the task, ignore the record, or report
  it through the available `ErrantRecordReporter`.
- A transient failure consumes the retry budget, sets `SinkTaskContext.timeout`,
  and throws Kafka's `RetriableException`. Exhaustion throws the connector's
  `TransientException`, which is not a `RetriableException`.
- Conversion or batch-level failures need not identify a single `errorRecord`.
  The current writer fails rather than attributing these to an arbitrary record;
  do not describe all `DataException`s as DLQ-eligible.

The writer checks transient result errors before processing data errors. A retry
can replay successful documents in the failed batch. When changing this logic,
cover mixed successes/data/transient errors, the exact reported record, progress
across multiple batches and repeated `put` calls, retry exhaustion, and reset
following recovery. Do not skip the failed batch, restart already completed
batches accidentally, or treat tolerated errors as permission to swallow
transport failures. Use Connect's retry mechanism rather than adding sleeps or
an independent retry loop in the task.

## Lifecycle and concurrency

Keep monitor/client/executor ownership explicit through startup failure, normal
stop, and reconfiguration. Review mutable endpoint-list access as well as the
synchronized methods: a synchronized getter does not make later caller mutation
safe. Preserve task endpoint rotation and the distinction between host-list
acquisition and scheduled connection rebalancing. Exercise discovery changes and
unchanged/empty responses, plus cleanup and reconfiguration callbacks where
relevant, using `HostListMonitorTest` and `LoadBalancingTest` as entry points.

## Build and packaging

Use `pom.xml` and the assembly descriptor, not a standalone `javac` invocation.
Keep explicit dependency scopes, dependency convergence, and compile-scope
Java 8 bytecode enforcement. The compiler's source/target settings are not
`--release`; retain Java 8 API compatibility in connector code too.

The connector uses the shaded ArangoDB driver with JSON/VPACK serde dependencies.
Kafka APIs, Jackson core, and SLF4J are worker-provided; test runtime dependencies
are not plugin dependencies. Preserve those boundaries and `META-INF/services`
merging when changing the assembly. Validate the packaged plugin in distributed
workers: an embedded test can pass while the deployed artifact lacks providers
or conflicts with worker libraries.

Use `-Ddistributed` to select the distributed test source/resource set. Do not
move those fixtures into production or retain compiled providers from the other
profile. For dependency upgrades, select the applicable JDK, Kafka-image, and
ArangoDB lanes from the testing guide; do not silence enforcer failures to obtain
a green build. Edit the version template/POM rather than its generated output.
