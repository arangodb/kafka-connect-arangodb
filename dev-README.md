# dev-README

Unit tests:
```shell
mvn test
```

Package:
```shell
mvn package
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
mvn clean integration-test -Pstandalone -Darango.endpoints=172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549
```

Integration tests with cluster Kafka Connect:
```shell
mvn clean integration-test -Pcluster -Darango.endpoints=172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549
```
