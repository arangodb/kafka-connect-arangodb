#!/bin/bash

SCHEMA_REGISTRY_IP=172.28.11.21 # port 8081
KAFKA_BOOTSTRAP_SERVERS=PLAINTEXT://172.28.11.1:9092,PLAINTEXT://172.28.11.2:9092,PLAINTEXT://172.28.11.3:9092

docker run -d \
  --name schema-registry -h schema-registry \
  --network arangodb --ip "$SCHEMA_REGISTRY_IP" \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e SCHEMA_REGISTRY_HOST_NAME=localhost \
  docker.io/confluentinc/cp-schema-registry:7.4.0

