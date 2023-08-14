#!/bin/bash

# exit when any command fails
set -e

KAFKA_VERSION=${KAFKA_VERSION:=latest}
DOCKER_IMAGE=docker.io/bitnami/kafka:$KAFKA_VERSION
docker pull $DOCKER_IMAGE

KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
LOCATION=$(pwd)/$(dirname "$0")

# data volume 1
docker create -v /tmp --name kafka-connect-data-1 alpine:3 /bin/true
docker cp "$LOCATION"/../target kafka-connect-data-1:/tmp/kafka-connect-arangodb
docker cp "$LOCATION"/../src/test/resources/test.truststore kafka-connect-data-1:/tmp
docker cp "$LOCATION"/connect/connect-distributed-1.properties kafka-connect-data-1:/tmp

docker run -d \
  --name kafka-connect-1 -h kafka-connect-1 \
  --network arangodb \
  -p 18083:8083 \
  --volumes-from kafka-connect-data-1 \
  -e BITNAMI_DEBUG=true \
  $DOCKER_IMAGE \
  /opt/bitnami/kafka/bin/connect-distributed.sh /tmp/connect-distributed-1.properties

# data volume 2
docker create -v /tmp --name kafka-connect-data-2 alpine:3 /bin/true
docker cp "$LOCATION"/../target kafka-connect-data-2:/tmp/kafka-connect-arangodb
docker cp "$LOCATION"/../src/test/resources/test.truststore kafka-connect-data-2:/tmp
docker cp "$LOCATION"/connect/connect-distributed-2.properties kafka-connect-data-2:/tmp

docker run -d \
  --name kafka-connect-2 -h kafka-connect-2 \
  --network arangodb \
  -p 28083:8083 \
  --volumes-from kafka-connect-data-2 \
  -e BITNAMI_DEBUG=true \
  $DOCKER_IMAGE \
  /opt/bitnami/kafka/bin/connect-distributed.sh /tmp/connect-distributed-2.properties

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
