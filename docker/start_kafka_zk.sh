#!/bin/bash

# exit when any command fails
set -e

KAFKA_VERSION=${KAFKA_VERSION:=3.9}
DOCKER_IMAGE=docker.io/bitnamilegacy/kafka:$KAFKA_VERSION
docker pull $DOCKER_IMAGE

ZK_DOCKER_IMAGE=docker.io/bitnamilegacy/zookeeper:3.9
docker pull $ZK_DOCKER_IMAGE

docker run -d \
  --name kafka-zk-1 -h kafka-zk-1 \
  --network arangodb \
  -e BITNAMI_DEBUG=true \
  -e ALLOW_ANONYMOUS_LOGIN="yes" \
  $ZK_DOCKER_IMAGE

docker run -d \
  --name kafka-1 -h kafka-1 \
  --network arangodb \
  -p 19092:19092 \
  -e BITNAMI_DEBUG=true \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=kafka-zk-1 \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:19092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-1:9092,PLAINTEXT_HOST://172.28.0.1:19092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  $DOCKER_IMAGE

docker run -d \
  --name kafka-2 -h kafka-2 \
  --network arangodb \
  -p 29092:29092 \
  -e BITNAMI_DEBUG=true \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=kafka-zk-1 \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:29092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-2:9092,PLAINTEXT_HOST://172.28.0.1:29092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  $DOCKER_IMAGE

docker run -d \
  --name kafka-3 -h kafka-3 \
  --network arangodb \
  -p 39092:39092 \
  -e BITNAMI_DEBUG=true \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=kafka-zk-1 \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:39092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-3:9092,PLAINTEXT_HOST://172.28.0.1:39092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  $DOCKER_IMAGE

wait_server() {
    # shellcheck disable=SC2091
    until $(nc -z $1 $2); do
        printf '.'
        sleep 1
    done
}

echo "Waiting..."
wait_server 172.28.0.1 19092
wait_server 172.28.0.1 29092
wait_server 172.28.0.1 39092

echo ""
echo ""
echo "Done, your deployment is reachable at: "
echo "Kafka:          172.28.0.1:19092,172.28.0.1:29092,172.28.0.1:39092"
