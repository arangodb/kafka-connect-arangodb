# Kafka Connect ArangoDB Connector

The Kafka Connect ArangoDB Sink connector allows you to export data from Apache Kafka® to ArangoDB.
It writes data from one or more topics in Kafka to a collection in ArangoDB.

## Supported versions

This connector is compatible with:

- Kafka `2.x` (from version `2.6`) and Kafka `3.x` (all versions)
- JDK 8 and higher versions
- all the non-EOLed [ArangoDB versions](https://www.arangodb.com/eol-notice)

## Installation

Download the Jar file from [Maven Central](https://repo1.maven.org/maven2/com/arangodb/kafka-connect-arangodb) and copy
it into one of the directories that are listed in the Kafka Connect worker's `plugin.path` configuration property. This
must be done on each of the installations where Kafka Connect will be run.

Once installed, you can then create a connector configuration file with the connector's settings, and deploy that to a
Connect worker, see [configuration documentation](./configuration.md).

See [here](https://docs.confluent.io/platform/current/connect/userguide.html#connect-installing-plugins) for more
detailed instructions.

## Delivery Guarantees

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

## Error handling

The connector categorizes all the possible errors into 2 types: data errors and transient errors.

### Data Errors

These are errors that are unrecoverable and caused by the data being processed. For example:

- conversion errors:
    - illegal key type
    - illegal value type
- server validation errors:
    - illegal `_key`, `_from`, `_to` values
    - JSON schema validation errors
- server constraints violations
    - unique index violations
    - key conflicts (in case of `insert.overwriteMode=conflict`)

The configuration property `data.errors.tolerance` allows configuring the behavior for tolerating data errors:

- `none`: data errors will result in an immediate connector task failure (default)
- `all`: changes the behavior to skip over records generating data errors. If DLQ is configured, then the record will
  be reported (see [DLQ](#dead-letter-queue)).

Data errors detection can be further customized via the configuration property `extra.data.error.nums`.
In addition to the cases listed above, the server errors reporting `errorNums` listed by this configuration property
will be considered data errors.

### Transient Errors

These are errors that are recoverable and could succeed if retried with some delay (see [Retries](#retries)).
If all retries fail, then the connector task will fail.

All errors that are not data errors are considered transient errors.

## Retries

In case of transient errors, the configuration property `max.retries` (default `10`) determines how many times the
connector will retry.

The configuration property `retry.backoff.ms` (default `3000`) allows setting the time in milliseconds to wait following
an error before a retry attempt is made.

Data errors are never retried.

## Dead Letter Queue

This connector supports the Dead Letter Queue (DLQ) functionality.
For information about accessing and using the DLQ,
see [Confluent Platform Dead Letter Queue](https://docs.confluent.io/platform/current/connect/concepts.html#dead-letter-queue).

Only data errors can be reported to the DLQ. Transient errors, after potential retries, will always make the task fail.

DLQ support for data errors can be enabled by setting `data.errors.tolerance=all`
and `errors.deadletterqueue.topic.name`.

## Multiple tasks

The ArangoDB Sink connector supports running one or more tasks. You can specify the number of tasks in the `tasks.max`
configuration parameter.

## Data mapping

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

## Key handling

The `_key` of the documents inserted into ArangoDB is derived in the following way:

1. use the Kafka record value field `_key` if present and not null, else
2. use the Kafka record key if not null, else
3. use the Kafka record coordinates (`topic-partition-offset`)

## Delete mode

The connector can delete documents in a database collection when it consumes a tombstone record, which is a Kafka record
that has a non-null key and a null value. This behavior is disabled by default, meaning that any tombstone records will
result in a failure of the connector.

Deletes can be enabled with `delete.enabled=true`.

Enabling delete mode does not affect the `insert.overwriteMode`.

## Write Modes

The configuration parameter `insert.overwriteMode` allows setting the write behavior in case a document with the
same `_key` already exists:

- `conflict`: the new document value is not written and an exception is thrown (default)
- `ignore`: the new document value is not written
- `replace`: the existing document is overwritten with the new document value
- `update`: the existing document is patched (partially updated) with the new document value

## Idempotent writes

All the write modes supported are idempotent, with the exception that the document revision field (`_rev`) will change
every time a document is written.
See [related documentation](https://www.arangodb.com/docs/stable/data-modeling-documents.html#document-revisions) for
more details.

If there are failures, the Kafka offset used for recovery may not be up-to-date with what was committed as of the time
of the failure, which can lead to re-processing during recovery. In case of `insert.overwriteMode=conflict` (default),
this can lead to constraint violations errors if records need to be re-processed.

## Ordering Guarantees

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

## Monitoring

The Kafka Connect framework exposes basic status information over a REST interface. Fine-grained metrics, including the
number of processed messages and the rate of processing, are available via JMX. For more information, see
[Monitoring Kafka Connect and Connectors](https://docs.confluent.io/current/connect/managing/monitoring.html)
(published by Confluent, also applies to a standard Apache Kafka distribution).

## SSL

To connect to ArangoDB using an SSL connection, the configuration property `ssl.enabled` must be set to `true`.

### Certificate from file

The connector can load the trust store to be used from file. The following configuration properties can be used:

- `ssl.truststore.location`: the location of the trust store file
- `ssl.truststore.password`: the password for the trust store file

Note that the trust store file path will need to be accessible from all Kafka Connect workers.

### Certificate from configuration property value

The connector can accept the SSL certificate value from configuration property encoded as base64 string. The following
configuration properties can be used:

- `ssl.cert.value`: base64 encoded SSL certificate

See [SSL configuration](./configuration.md#ssl) for further options.

## Current limitations

- `VST` communication protocol is currently not working (DE-619)
- documents are inserted one by one, bulk inserts will be implemented in a future release (DE-627)
- in case of transient error, the entire Kafka Connect batch is retried (DE-651)
- record values are required to be object-like structures (DE-644)
- auto-creation of ArangoDB collection is not supported (DE-653)
- `ssl.cert.value` does not support multiple certificates (DE-655)
- batch inserts are not guaranteed to be executed serially
- batch inserts could succeed for some documents while failing for others. This has 2 important consequences:
    - transient errors might be retried and succeed at a later point
    - data errors might be asynchronously reported to DLQ

## Demo

Check out our [demo](https://github.com/arangodb/kafka-connect-arangodb/tree/main/demo) to learn more about Kafka
Connect ArangoDB Connector.
