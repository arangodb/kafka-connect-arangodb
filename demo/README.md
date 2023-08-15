# kafka-connect-arangodb demo

## Prepare the environment

Set package version:

```shell
export PACKAGE_VERSION=1.0.0-RC.1
```

Create the Docker network:

```shell
./create_network.sh 
```

Start ArangoDB cluster:

```shell
STARTER_MODE=cluster ./start_db.sh
```

The deployed cluster will be accessible at [http://172.28.0.1:8529](http://172.28.0.1:8529) with username `root` and
password `test`.

Download package:

```shell
wget -P ./data/connectors/kafka-connect-arangodb/ "https://repo1.maven.org/maven2/com/arangodb/kafka-connect-arangodb/$PACKAGE_VERSION/kafka-connect-arangodb-$PACKAGE_VERSION.jar"
```

Start docker compose environment:
- Kafka cluster
- Kafka Connect cluster
- Redpanda Console

```shell
docker compose up
```

The console will be accessible at [http://172.28.0.1:8080](http://172.28.0.1:8080).


## Produce data

Create source connector:

```shell
curl --request POST \
    --url "http://172.28.0.1:18083/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "source-datagen-orders",
        "config": {
          "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
          "kafka.topic": "orders",
          "quickstart": "orders",
          "topic.creation.default.partitions": "3",
          "topic.creation.default.replication.factor": "1"
        }
    }'
```

The messages produced can be checked at [http://172.28.0.1:8080/topics/orders](http://172.28.0.1:8080/topics/orders).


## Create sink connector

Create db collection:

```shell
curl -u root:test http://172.28.0.1:8529/_api/collection -d '{"name": "orders"}'
```

Explore configuration options in the console at [http://172.28.0.1:8080/connect-clusters/kafka-connect/create-connector](http://172.28.0.1:8080/connect-clusters/kafka-connect/create-connector)
or via:

```shell
curl http://172.28.0.1:18083/connector-plugins/ArangoSinkConnector/config | jq
```

Create sink connector:

```shell
curl --request POST \
    --url "http://172.28.0.1:18083/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "sink-adb-orders",
        "config": {
          "connector.class": "com.arangodb.kafka.ArangoSinkConnector",
          "tasks.max": 2,
          "topics": "orders",
          "connection.endpoints": "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549",
          "connection.password": "test",
          "connection.collection": "orders",
          "insert.overwriteMode": "REPLACE"
        }
    }'
```

Check documents count:

```shell
curl -u root:test http://172.28.0.1:8529/_api/cursor -d '{"query":"FOR d IN orders COLLECT WITH COUNT INTO c RETURN c"}'
```

Check inserted documents at [http://172.28.0.1:8529/_db/_system/_admin/aardvark/index.html#collection/orders/documents/1](http://172.28.0.1:8529/_db/_system/_admin/aardvark/index.html#collection/orders/documents/1).
