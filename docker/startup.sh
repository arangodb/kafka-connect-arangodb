#!/bin/bash

KC=${KC:=false}

./docker/create_network.sh

# exit when any command fails
set -e

./docker/start_db.sh &
./docker/start_kafka_zk.sh
if [ "$KC" == "true" ]; then
  ./docker/start_kafka_connect.sh &
fi
./docker/start_schema_registry.sh &

wait
