#!/bin/bash

DOCKER_IMAGE=docker.io/redpandadata/console:v3.3.2
docker pull $DOCKER_IMAGE

docker run -d \
  --name redpanda-console -h redpanda-console \
  --network arangodb \
  -p 8080:8080 \
  -e KAFKA_BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092" \
  -e CONNECT_ENABLED="true" \
  -e CONNECT_CLUSTERS_NAME="kafka-connect" \
  -e CONNECT_CLUSTERS_URL="http://kafka-connect-1:8083" \
  $DOCKER_IMAGE
