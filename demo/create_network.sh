#!/bin/bash

docker network create arangodb --subnet 172.28.0.0/16 --gateway 172.28.0.1
