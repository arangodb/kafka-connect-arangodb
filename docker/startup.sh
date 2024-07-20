#!/bin/bash

KC=${KC:=false}

./docker/create_network.sh

# exit when any command fails
set -e

./docker/start_db.sh &
START_DB_PID=$!

./docker/start_kafka_zk.sh
./docker/start_schema_registry.sh &
START_SCHEMA_REGISTRY_PID=$!

if [ "$KC" == "true" ]; then
  ./docker/start_kafka_connect.sh
fi

wait $START_DB_PID
wait $START_SCHEMA_REGISTRY_PID
