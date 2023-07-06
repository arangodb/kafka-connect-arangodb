#!/bin/bash

KAFKA_IP=172.28.11.1 # port 9092
KAFKA_CONNECT_IP=172.28.11.11 # port 8083
LOCATION=$(pwd)/$(dirname "$0")

docker run -d \
  --name kafka-connect-1 -h kafka-connect-1 \
  --network arangodb --ip "$KAFKA_CONNECT_IP" \
  -v "$LOCATION"/../target/kafka-connect-arangodb-1.0.0-SNAPSHOT.jar:/usr/share/java/kafka-connect-arangodb/kafka-connect-arangodb-1.0.0-SNAPSHOT.jar \
  -e CONNECT_BOOTSTRAP_SERVERS="$KAFKA_IP:9092" \
  -e CONNECT_GROUP_ID="kafka-connect" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="kafka-connect.config" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="kafka-connect.offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="kafka-connect.status" \
  -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME="$KAFKA_CONNECT_IP" \
  -e CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN="%d %p %X{connector.context} %c:%L - %m%n" \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR="1" \
  -e CONNECT_PLUGIN_PATH="/usr/share/java" \
  confluentinc/cp-kafka-connect:7.4.0

wait_server() {
    # shellcheck disable=SC2091
    until $(curl --output /dev/null --fail --silent --head -i "$1"); do
        printf '.'
        sleep 1
    done
}

echo "Waiting..."
wait_server "http://$KAFKA_CONNECT_IP:8083"

echo ""
echo ""
echo "Done, your deployment is reachable at: "
echo "Kafka Connect:  $KAFKA_CONNECT_IP:8083"
