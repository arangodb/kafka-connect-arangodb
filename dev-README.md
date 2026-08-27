# dev-README

Unit tests:
```shell
mvn test
```

Package:
```shell
mvn package -Ddistributed
```

Create docker network (once until reboot):
```shell
./docker/create_network.sh
```

Start test environment:
```shell
KC=true STARTER_MODE=cluster ./docker/startup.sh
```

Integration tests with standalone Kafka Connect:
```shell
mvn integration-test
```

Integration tests with cluster Kafka Connect:
```shell
mvn integration-test -Ddistributed -Darango.topology=cluster
```

### Diagnose an integration-test record timeout

The trace contains `SEND`, `BROKER_ACK`,
`CONNECT_RECEIVED`, and `ARANGODB_WRITTEN` entries, all carrying the Kafka
topic/partition/offset (and, when available, the document key).

```shell
RECORD_TRACE=true KC=true STARTER_MODE=cluster ./docker/startup.sh
mvn integration-test -Drecord.trace.level=DEBUG
```

`BROKER_ACK` proves the broker accepted the record.  If it is present but no
`CONNECT_RECEIVED` appears in `kafka-connect-*`, inspect the Connect worker or
its consumer assignment; if `CONNECT_RECEIVED` appears but
`ARANGODB_WRITTEN` does not, the problem is in connector processing or its
ArangoDB call.

## check dependencies updates
```shell
mvn versions:display-dependency-updates
mvn versions:display-plugin-updates
```
