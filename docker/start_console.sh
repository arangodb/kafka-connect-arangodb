#!/bin/bash

docker run -d \
  --name redpanda-console -h redpanda-console \
  --network arangodb \
  -p 8080:8080 \
  -e KAFKA_BROKERS="kafka-0:9092" \
  -e CONNECT_ENABLED="true" \
  -e CONNECT_CLUSTERS_NAME="kafka-connect" \
  -e CONNECT_CLUSTERS_URL="http://kafka-connect-1:8083" \
  docker.io/redpandadata/console:v2.2.4
