# Kafka Connect ArangoDB Connector

The Kafka Connect ArangoDB Sink connector allows you to export data from Apache Kafka® to ArangoDB.
It writes data from one or more topics in Kafka to a collection in Elasticsearch.

Auto-creation of ArangoDB collection is not supported.

## Features

The ArangoDB Sink connector includes the following features:

    Delivery Guarantees
    Error handling
    Retries
    Dead Letter Queue
    Multiple tasks
    Data mapping
    Key handling
    Delete mode
    Idempotent writes
    Ordering Guarantees
    Monitoring
    Compatibility

### Delivery Guarantees

This connector guarantees that each record in the Kafka topic is delivered at least once.
For example, the same record could be delivered multiple times in the following scenarios:

- transient errors in the communication between the connector and the db, leading to [retries](#retries)
- errors in the communication between the connector and Kafka, preventing to commit offset of already written records
- abrupt termination of connector task

When restarted, the connector resumes reading from the Kafka topic at an offset prior to where it stopped.
As a result, at least in the cases mentioned above some records might get written to ArangoDB more than once.
Even if configured for idempotent writes (e.g. with `insert.overwriteMode=replace`), writing the same record multiple
times will still update the `_rev` field of the document.

Note that in case of retries, [Ordering Guarantees](#ordering-guarantees) are still provided.

To improve the likelihood that every write survives even in case of a db-server failover, consider configuring the
configuration property `insert.waitForSync` (default `false`), which determines whether the write operations are synced
to disk before returning.

### Error handling

TODO, see DE-654

The connector categorize all the possible errors into 2 types:

- `transient errors`: errors that are recoverable and could be retried, e.g. timeout errors
- `fatal errors`: errors that are unrecoverable and will not be retried, e.g. illegal document keys

The configuration property `fatal.errors.tolerance`
Default:    none
Behavior for tolerating errors during connector operation.

- ‘none’ is the default value and signals that any error will result in an immediate connector task failure;
- ‘all’ changes the behavior to skip over problematic records, if DLQ is configured, then the record will be reported (
  see DLQ)

`fatal.errors.log.enable`
Default:    false
If true, write each error and the details of the failed operation and problematic record to the Connect application log.
This is ‘false’ by default, so that only errors that are not tolerated are reported.

TODO, see DE-654
extra.fatal.errorNums
extra.transient.errorNums

### Retries

In case of transient errors, the configuration property `max.retries` (default `10`) determines how many times the
connector will retry.

The configuration property `retry.backoff.ms` (default `3000`) allows setting the time in milliseconds to wait following
an error before a retry attempt is made.

Fatal errors are not retried.

### Dead Letter Queue

This connector supports the Dead Letter Queue (DLQ) functionality.
For information about accessing and using the DLQ,
see [Confluent Platform Dead Letter Queue](https://docs.confluent.io/platform/current/connect/concepts.html#dead-letter-queue).

TODO

To enable it:

- `fatal.errors.tolerance=all`
- errors.deadletterqueue.topic.name
- errors.deadletterqueue.topic.replication.factor
- errors.deadletterqueue.context.headers.enable

### Multiple tasks

The ArangoDB Sink connector supports running one or more tasks. You can specify the number of tasks in the `tasks.max`
configuration parameter.

### Data mapping

The sink connector optionally supports schemas. For example, the Avro converter that comes with Schema Registry, the
JSON converter with schemas enabled, or the Protobuf converter.

Kafka record keys and Kafka record value field `_key`, if present, must be a primitive type of either:

- `string`
- `integral numeric` (integer)

The record value must be either:

- `struct` (Kafka Connect structured record)
- `map`
- `null` (tombstone record)

If the data in the topic is not of a compatible format, applying
an [SMT](https://docs.confluent.io/platform/current/connect/transforms/overview.html) or implementing a custom converter
may be necessary.

### Key handling

The `_key` of the documents inserted into ArangoDB is derived in the following way:

1. use the Kafka record value field `_key` if present and not null, else
2. use the Kafka record key if not null, else
3. use the Kafka record coordinates (`topic-partition-offset`)

### Delete mode

The connector can delete documents in a database collection when it consumes a tombstone record, which is a Kafka record
that has a non-null key and a null value. This behavior is disabled by default, meaning that any tombstone records will
result in a failure of the connector.

Deletes can be enabled with `delete.enabled=true`.

Enabling delete mode does not affect the `insert.overwriteMode`.

### Idempotent writes

TODO

### Ordering Guarantees

Kafka records in the same Kafka topic partition mapped to documents with the same `_key` (see
[Key handling](#key-handling)) will be written to ArangoDB in the same order as they are in the Kafka topic partition.

The order between writes for records in the same Kafka partition that are mapped to documents with different `_key` is
not guaranteed.

The order between writes for records in different Kafka partitions is not guaranteed.

To guarantee documents in ArangoDB are eventually consistent with the records in the Kafka topic, it is recommended
deriving the document `_key` from Kafka record keys and using a key-based partitioner that assigns the same partition to
records with the same key (e.g. Kafka default partitioner).

Otherwise, in case the document `_key` is assigned from Kafka record value field `_key`, the same could be achieved
using a field partitioner on `_key`.

When restarted, the connector could resume reading from the Kafka topic at an offset prior to where it stopped.
This could lead to reprocessing of batches containing multiple Kafka records that are mapped to documents with the
same `_key`.
In such case, it will be possible to observe the related document in the database being temporarily updated to older
versions and eventually to newer ones.

### Monitoring

The Kafka Connect framework exposes basic status information over a REST interface. Fine-grained metrics, including the
number of processed messages and the rate of processing, are available via JMX. For more information, see
[Monitoring Kafka Connect and Connectors](https://docs.confluent.io/current/connect/managing/monitoring.html)
(published by Confluent, also applies to a standard Apache Kafka distribution).

### Compatibility

TODO

- Kafka versions: TODO after DE-633
- Non-EOLed versions of ArangoDB 3.11+
- Java 8+
- AGIP
