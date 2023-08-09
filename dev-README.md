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

Standalone integration tests:
```shell
mvn integration-test -Darango.endpoints=172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549
```

Cluster integration tests:
```shell
mvn integration-test -Dkafka.connect.host=http://172.28.0.1:18083 -Darango.endpoints=172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549
```
