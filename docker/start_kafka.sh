#!/bin/bash

DOCKER_IMAGE=docker.io/bitnami/kafka:3.4
docker pull $DOCKER_IMAGE

docker run -d \
  --name kafka-1 -h kafka-1 \
  --network arangodb \
  -p 19092:19092 \
  -e BITNAMI_DEBUG=true \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:19092,CONTROLLER://:9093 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093" \
  -e KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-1:9092,PLAINTEXT_HOST://127.0.0.1:19092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT \
  $DOCKER_IMAGE

docker run -d \
  --name kafka-2 -h kafka-2 \
  --network arangodb \
  -p 29092:29092 \
  -e BITNAMI_DEBUG=true \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_NODE_ID=2 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:29092,CONTROLLER://:9093 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093" \
  -e KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-2:9092,PLAINTEXT_HOST://127.0.0.1:29092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT \
  $DOCKER_IMAGE

docker run -d \
  --name kafka-3 -h kafka-3 \
  --network arangodb \
  -p 39092:39092 \
  -e BITNAMI_DEBUG=true \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_NODE_ID=3 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://0.0.0.0:39092,CONTROLLER://:9093 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093" \
  -e KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://kafka-3:9092,PLAINTEXT_HOST://127.0.0.1:39092" \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT \
  $DOCKER_IMAGE

echo ""
echo ""
echo "Done, your deployment is reachable at: "
echo "Kafka:          127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
