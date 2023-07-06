# dev-README

Unit tests:
```shell
mvn test
```

Package:
```shell
mvn package
```

Start db:
```shell
./docker/start_db.sh
```

Start Kafka:
```shell
./docker/start_kafka.sh
```

Standalone integration tests:
```shell
mvn integration-test
```

Start Kafka Connect:
```shell
./docker/start_kafka_connect.sh
```

Cluster integration tests:
```shell
mvn integration-test -Dkafka.connect.host=http://172.28.11.11:8083
```
