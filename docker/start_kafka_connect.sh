#!/bin/bash

DOCKER_IMAGE=docker.io/confluentinc/cp-kafka-connect:7.4.0
docker pull $DOCKER_IMAGE

KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
LOCATION=$(pwd)/$(dirname "$0")

docker run -d \
  --name kafka-connect-1 -h kafka-connect-1 \
  --network arangodb \
  -p 18083:8083 \
  -v "$LOCATION"/../target:/usr/share/java/kafka-connect-arangodb \
  -v "$LOCATION"/../src/test/resources/test.truststore:/tmp/test.truststore \
  -e CONNECT_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e CONNECT_GROUP_ID="kafka-connect" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="kafka-connect.config" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="kafka-connect.offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="kafka-connect.status" \
  -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect-1" \
  -e CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN="%d %p %X{connector.context} %c:%L - %m%n" \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_PLUGIN_PATH="/usr/share/java" \
  $DOCKER_IMAGE

docker run -d \
  --name kafka-connect-2 -h kafka-connect-2 \
  --network arangodb \
  -p 28083:8083 \
  -v "$LOCATION"/../target:/usr/share/java/kafka-connect-arangodb \
  -v "$LOCATION"/../src/test/resources/test.truststore:/tmp/test.truststore \
  -e CONNECT_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e CONNECT_GROUP_ID="kafka-connect" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="kafka-connect.config" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="kafka-connect.offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="kafka-connect.status" \
  -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect-2" \
  -e CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN="%d %p %X{connector.context} %c:%L - %m%n" \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_PLUGIN_PATH="/usr/share/java" \
  $DOCKER_IMAGE

wait_server() {
    # shellcheck disable=SC2091
    until $(curl --output /dev/null --fail --silent --head -i "$1"); do
        printf '.'
        sleep 1
    done
}

echo "Waiting..."
wait_server "http://172.28.0.1:18083"
wait_server "http://172.28.0.1:28083"

echo ""
echo ""
echo "Done, your deployment is reachable at: "
echo "Kafka Connect:  http://172.28.0.1:18083"
echo "Kafka Connect:  http://172.28.0.1:28083"
