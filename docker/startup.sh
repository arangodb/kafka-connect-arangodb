#!/bin/bash

KC=${KC:=false}

./docker/create_network.sh

# exit when any command fails
set -e

./docker/start_kafka.sh &
./docker/start_db.sh &
if [ "$KC" == "true" ] || [ ! -z "$KAFKA_CONNECT_HOST" ]; then
  sleep 1 && ./docker/start_kafka_connect.sh &
fi
sleep 1 && ./docker/start_schema_registry.sh &

wait
