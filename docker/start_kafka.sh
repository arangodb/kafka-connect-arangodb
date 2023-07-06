#!/bin/bash

KAFKA_IP=172.28.11.1 # port 9092

docker run -d \
  --name kafka-1 -h kafka-1 \
  --network arangodb --ip "$KAFKA_IP" \
  -e ALLOW_PLAINTEXT_LISTENER="yes" \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://$KAFKA_IP:9092" \
  docker.io/bitnami/kafka:3.4

echo ""
echo ""
echo "Done, your deployment is reachable at: "
echo "Kafka:          $KAFKA_IP:9092"
