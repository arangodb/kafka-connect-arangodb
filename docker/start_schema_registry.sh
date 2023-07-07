#!/bin/bash

KAFKA_IP=172.28.11.1 # port 9092
SCHEMA_REGISTRY_IP=172.28.11.21 # port 8081

docker run -d \
  --name schema-registry -h schema-registry \
  --network arangodb --ip "$SCHEMA_REGISTRY_IP" \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="PLAINTEXT://$KAFKA_IP:9092" \
  -e SCHEMA_REGISTRY_HOST_NAME=localhost \
  docker.io/confluentinc/cp-schema-registry:7.4.0

