#!/bin/bash

wait_server() {
    # shellcheck disable=SC2091
    until $(nc -z $1 $2); do
        printf '.'
        sleep 1
    done
}
a=$@
while test -n "$a"; do
    HOST=$1
    PORT=$2
    wait_server "${HOST}" "${PORT}"
    shift 2
    a=$@
done
