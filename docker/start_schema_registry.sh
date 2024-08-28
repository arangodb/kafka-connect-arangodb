#!/bin/bash

DOCKER_IMAGE=docker.io/confluentinc/cp-schema-registry:7.5.2
docker pull $DOCKER_IMAGE

KAFKA_BOOTSTRAP_SERVERS=PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9092,PLAINTEXT://kafka-3:9092

docker run -d \
  --name kafka-schema-registry -h kafka-schema-registry \
  --network arangodb \
  -p 8081:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e SCHEMA_REGISTRY_HOST_NAME=172.28.0.1 \
  $DOCKER_IMAGE

