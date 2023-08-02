# Kafka Connect ArangoDB Connector

The Kafka Connect ArangoDB Sink connector allows you to export data from Apache Kafka® to ArangoDB.
It writes data from one or more topics in Kafka to a collection in Elasticsearch.

Auto-creation of ArangoDB collection is not supported.

## Features

The ArangoDB Sink connector includes the following features:

    At least once delivery
    Dead Letter Queue
    Multiple tasks
    Data mapping
    Key handling
    Delete mode
    Idempotent writes

### At least once delivery

This connector guarantees that records are delivered at least once from the Kafka topic.

### Dead Letter Queue

This connector supports the Dead Letter Queue (DLQ) functionality.
For information about accessing and using the DLQ,
see [Confluent Platform Dead Letter Queue](https://docs.confluent.io/platform/current/connect/concepts.html#dead-letter-queue).

The connector categorize all the possible errors into 2 types:

- `transient errors`: errors that are recoverable and could be retried, e.g. timeout errors
- `fatal errors`: errors that are unrecoverable and will not be retried, e.g. invalid data format

The `transient.errors.tolerance` configuration property determines whether transient errors, after all potential retries
failed, will be tolerated:

- `none`: transient errors will cause a connector task failure
- `all`: transient errors will be reported to the DLQ

Fatal errors will always be reported to the DLQ.

The `max.retries` configuration property determines how many times the ArangoDB Sink connector will try to insert the
data before it sends the errant record to DLQ. Note that this retry only happens if it is a transient error.

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
3. use the Kafka coordinates (`topic-partition-offset`)

### Delete mode

The connector can delete documents in a database collection when it consumes a tombstone record, which is a Kafka record
that has a non-null key and a null value. This behavior is disabled by default, meaning that any tombstone records will
result in a failure of the connector.

Deletes can be enabled with `delete.enabled=true`.

Enabling delete mode does not affect the `insert.overwriteMode`.











### Idempotent writes






### Sequential consistency

updates to a key are written to ArangoDB in order

