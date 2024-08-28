#!/bin/bash

# exit when any command fails
set -e

until ./docker/startup.sh
do
    # rm all containers, in case of retries
    # retries might be needed in case of port-mapping errors in CircleCI, e.g. for errors like:
    # docker: Error response from daemon: driver failed programming external connectivity on endpoint kafka-3:
    # Error starting userland proxy: listen tcp4 0.0.0.0:39092: bind: address already in use.

    docker ps -a -f name=kafka-.* -q | while read x ; do docker rm -f $x ; done
    docker rm -f adb
    docker ps -a -f name=adb-.* -q | while read x ; do docker rm -f $x ; done
    echo "Startup failed, retrying in 10 seconds..."
    sleep 10
done
