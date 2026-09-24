# Build and test guide

Contents: [local checks](#local-checks), [infrastructure](#infrastructure),
[CI jobs](#ci-jobs), [matrix](#ci-matrix), [test placement](#test-placement-and-coverage),
[diagnostics](#diagnostics).

Use the checked-out `.circleci/config.yml`, `pom.xml`, and `docker/` scripts as
the authority when their configuration changes. The recipes below reproduce the
four test jobs; they do not replace them with a new runner.

## Local checks

Run from the repository root with Maven 3.9+ (`mvn`, no wrapper). Use JDK 21 for
the default CI jobs; the distributed JDK matrix also uses 17 and 25. The POM's
Java 8 source/target settings apply to production and test sources, not to the
JDK requirements of Kafka and the test dependencies.

```sh
mvn test
# Focused unit regression; substitute the affected classes/methods:
mvn test -Dtest=ConverterTest,ErrorHandlingTest
```

Surefire runs `*Test` unit tests without external services. Failsafe runs `*IT`
integration tests. Both its `integration-test` and `verify` goals are explicitly
bound to the **integration-test phase** in this POM, so `mvn integration-test`
checks integration-test failures. Plain `mvn verify` additionally triggers GPG
signing; do not substitute it or a deploy command for the CI test command.

Use `-Dtest=ClassName#method` for Surefire and `-Dit.test=WriteIT#delete` for
Failsafe. Add the latter to the relevant integration job's Maven command while
keeping its mode/topology/enablement properties. An integration selection does
not suppress unit tests in earlier lifecycle phases. Check fresh
`target/surefire-reports/` and `target/failsafe-reports/` for the intended cases
and skips; do not disable no-matching-test failures.

Use `mvn clean` before switching standalone/distributed modes or rebuilding a
changed artifact version. The profiles add different test sources and service
resources; stale classes or `target/jars/` can contaminate the next run. Use
`-Ddistributed`, not just `-Pdistributed`: test code also reads the property.

## Infrastructure

The CI integration harness starts external Kafka, Schema Registry, and ArangoDB
in both modes; only the Connect worker is embedded in standalone mode. Use a
disposable Linux/Docker environment reachable at the harness's bridge gateway
`172.28.0.1`. Check Docker access and the scripts' Bash, curl, and nc prerequisites;
resilience also needs wget and the Linux amd64 Toxiproxy binary downloaded by
`bin/startProxy.sh`. Do not assume Docker Desktop or a remote daemon exposes the
same routing to the Maven process.

Each recipe assumes fresh, unshared Docker resources and appropriate build
output. `startup_retry.sh` invokes `startup.sh`, which creates the network and
services; no separate network step is needed. Startup uses fixed names/ports,
mounts the Docker socket, sets test credentials, and may remove matching
containers on retry. Tests recreate named topics/collections. Inspect the scripts
before use; bound local startup retries rather than leaving a failed setup
looping indefinitely. CI's job-cancellation command is not a local timeout.

`DOCKER_IMAGE` selects ArangoDB; export the image of the desired CI case before
startup. An unset/empty value uses the script default,
`docker.io/arangodb/enterprise:latest`. Supply `ARANGO_LICENSE_KEY` where required
by the chosen Enterprise image; do not put it in repository files. CI's pipeline
`docker-img` parameter maps to this variable. Keep the selected image consistent
with the topology and TLS setup.

## CI jobs

### test-standalone

JDK 21, one ArangoDB server, embedded Connect. The explicit environment values
below make script defaults independent of a preceding local run.

```sh
KC=false SSL=false STARTER_MODE=single KAFKA_VERSION=4.2.0 ./docker/startup_retry.sh
mvn integration-test
```

### test-distributed

Select the JDK/image/topology/Kafka combination from the matrix below. Package
**before** startup, as CI does; two Docker Connect workers load copied JARs.

```sh
export STARTER_MODE=single KAFKA_VERSION=4.2.0
mvn package -Ddistributed
KC=true SSL=false ./docker/startup_retry.sh
mvn integration-test -Ddistributed -Darango.topology="$STARTER_MODE"
```

Use `STARTER_MODE=cluster` for a cluster lane, retaining the matching
`-Darango.topology`. `KC=true` starts workers but does not select the Maven test
provider; `-Ddistributed` selects that provider but does not start workers.
`start_kafka_connect.sh` copies `target/*.jar` through `target/jars/` into worker
data containers. After changing connector code/dependencies, rebuild and recreate
the run-owned workers **and their copied plugin data** before testing. Re-running
Maven or merely restarting a worker can still test the old JAR.

### test-ssl

JDK 21, distributed Connect, TLS-enabled single ArangoDB server. Both server TLS
setup and the case-sensitive JUnit enablement property are required.

```sh
mvn package -Ddistributed
KC=true SSL=true STARTER_MODE=single KAFKA_VERSION=4.2.0 ./docker/startup_retry.sh
mvn integration-test -Ddistributed -DSslTest=true -Dit.test=com.arangodb.kafka.SslIT
```

### test-resilience

JDK 21, clustered ArangoDB, **standalone** Connect. Do not add `-Ddistributed` or
`KC=true`: the proxies and logger assertions run in the test process's environment.

```sh
KC=false SSL=false STARTER_MODE=cluster KAFKA_VERSION=4.2.0 ./docker/startup_retry.sh
```

Start the proxy in another terminal and keep it running for the test command
(CircleCI runs this as a background step):

```sh
TOXIPROXY_VERSION=v2.7.0 ./bin/startProxy.sh
```

Once its API responds, run in the original terminal:

```sh
curl --fail http://127.0.0.1:8474/version
mvn integration-test -DresilienceTests -Darango.topology=cluster '-Dit.test=com.arangodb.kafka.resilience.**'
```

The quoted selector is CI's package pattern without shell glob expansion.
`-DresilienceTests` both enables the JUnit cases and selects proxied endpoints;
`arango.topology=cluster` supplies multiple endpoints for `FailoverIT`. Selecting
the classes alone can yield skipped tests. Stop the proxy after the run.

## CI matrix

These are separate workflow matrices, **not** one full Cartesian product.
Unless a row overrides them, distributed jobs use JDK 21, `single`, Kafka 4.2.0,
and the pipeline's ArangoDB image (or the script default).

| Workflow | Variants |
| --- | --- |
| `test-distributed-jdk-versions` | Host JDK 17, 21, 25. |
| `test-distributed-adb-versions` | When no pipeline image is supplied: `docker.io/arangodb/enterprise:3.12` and `docker.io/arangodb/core-preview:4-nightly`, each with `single` and `cluster`. |
| `test-distributed-adb-topologies` | When a pipeline image is supplied: that image with `single` and `cluster`. |
| `test-distributed-kafka-versions` | Host JDK 17 with Kafka 4.2.0, 4.1.1, and 4.0.1. |

`KAFKA_VERSION` selects broker/worker Docker images. It does **not** override the
POM's `kafka.version`, which controls compilation and embedded test dependencies.
Likewise the executor JDK is the Maven host JDK, not a replacement for the JDK
inside worker images. Do not conflate these dimensions when reporting coverage.
The `deploy` job (JDK 17, signing and Maven Central credentials) is a release job,
not another test lane.

## Test placement and coverage

Paths below are relative to `src/test/java/com/arangodb/kafka/`.

| Change | Start with | Broader check when relevant |
| --- | --- | --- |
| Configuration, protocol, TLS | `config/ArangoSinkConfigTest` | `ProtocolIT`, `SslIT`; image/TLS lanes. |
| Record/key conversion | `ConverterTest` | `ConverterIT` in standalone and distributed modes. |
| Batching, overwrites, deletes, errors | `ErrorHandlingTest` | `WriteIT`, `OverwriteModeIT`, `DlqIT`. |
| Discovery, rotation, lifecycle | `HostListMonitorTest`, `LoadBalancingTest`, `ArangoSinkConnectorTest`, `ArangoSinkTaskTest` | Distributed cluster; resilience for failover/retry changes. |
| Retry/backoff/failover | `ErrorHandlingTest` | `resilience/RetryIT`, `resilience/FailoverIT` with proxy setup. |
| Dependencies or assembly | Unit tests and package | Distributed plugin loading plus affected JDK/Kafka/server lanes. |

Use JUnit Jupiter, AssertJ, Mockito, and Awaitility as neighboring tests do.
`@KafkaTest` uses `TargetProvider` to instantiate target classes, set up services'
test data, create a connector, and inject parameters. Extend a target or target
group for a new configuration/converter/protocol case rather than duplicating
the lifecycle. Keep assertions about delivered documents, errors, or callbacks,
not just successful setup.

JUnit parallel execution is enabled. Keep names isolated via the existing target
naming and retain `@MockTest` isolation and `@ResourceLock("resilienceTests")` for
shared resources. Do not run separate Maven integration suites concurrently
against the same harness. Prefer bounded condition polling to new fixed sleeps.

Read target gates before claiming coverage: `AvroTarget` runs only in standalone
mode; `VstTarget` skips servers 3.12 and newer; `SslIT` and resilience classes are
opt-in. The checked-in ArangoDB version matrix therefore does not exercise VST.
A VST-specific change needs a compatible disposable server or an explicit report
that the path was not exercised. Do not remove capability gates to inflate counts.

## Diagnostics

Inspect fresh test reports and the failing service's logs first. CI renders
HTML with `mvn surefire-report:failsafe-report-only surefire-report:report-only`
under `target/site/`. On failure it calls `docker/stop_db.sh` to attempt a
`result.tar.gz` database-data archive; that script is **not** a full harness
teardown, and it reads `adb-data` although `start_db.sh` names the volume
container `arangodb-data`.

The harness owns the `adb` Starter container and the `adb-*` server containers it
creates, `arangodb-data`, the `kafka-*` containers (brokers, Schema Registry,
Connect workers and their data containers), and the `arangodb` network. Remove
only these; do not use machine-wide Docker prune. The retry loop does not remove
`arangodb-data`, so startup over leftovers from an earlier run retries forever
until that container is removed.

For record-delivery timeouts, add `-Drecord.trace.level=DEBUG` to the test command.
`src/test/resources/logback-test.xml` enables record-flow logs in the **test JVM**.
`SEND` precedes producer delivery, `BROKER_ACK` carries the broker position,
`CONNECT_RECEIVED` records task input, and `ARANGODB_WRITTEN` records insert-batch
collection/keys after result handling. The last marker is not a Kafka offset
commit, does not cover deletes, and is not proof that every tolerated record was
written. Correlate positions and keys rather than assuming all markers contain
identical fields.

In distributed mode, enable `com.arangodb.kafka.recordflow` in the worker's own
logging configuration to see connector-side markers. A Maven system property
does not reconfigure container logging, and the checked-in startup scripts do
not consume `RECORD_TRACE`. Do not diagnose a missing marker as a processing
failure until logging is enabled in the process that would emit it.
