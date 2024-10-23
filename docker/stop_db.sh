#!/bin/bash -x

docker stop adb

docker cp adb-data:/data - > result.tar
gzip result.tar
