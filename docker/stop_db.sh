#!/bin/bash -x
# Configuration environment variables:
#   STARTER_MODE:             (single|cluster|activefailover), default single
#   DOCKER_IMAGE:             ArangoDB docker image, default docker.io/arangodb/arangodb:latest
#   SSL:                      (true|false), default false
#   EXTENDED_NAMES:  (true|false), default false
#   ARANGO_LICENSE_KEY:       only required for ArangoDB Enterprise

# EXAMPLE:
# STARTER_MODE=cluster SSL=true ./start_db.sh

STARTER_MODE=${STARTER_MODE:=single}
DOCKER_IMAGE=${DOCKER_IMAGE:=docker.io/arangodb/arangodb:latest}
SSL=${SSL:=false}
EXTENDED_NAMES=${EXTENDED_NAMES:=false}

STARTER_DOCKER_IMAGE=docker.io/arangodb/arangodb-starter:latest
GW=172.28.0.1

echo "---"
echo "starting ArangoDB with:"
echo "DOCKER_IMAGE=$DOCKER_IMAGE"
echo "STARTER_DOCKER_IMAGE=$STARTER_DOCKER_IMAGE"
echo "STARTER_MODE=$STARTER_MODE"
echo "SSL=$SSL"
echo "EXTENDED_NAMES=$EXTENDED_NAMES"

# exit when any command fails
set -e

docker pull $STARTER_DOCKER_IMAGE
docker pull $DOCKER_IMAGE

LOCATION=$(pwd)/$(dirname "$0")
AUTHORIZATION_HEADER=$(cat "$LOCATION"/jwtHeader)

STARTER_ARGS=
SCHEME=http
ARANGOSH_SCHEME=http+tcp
COORDINATORS=("$GW:8529" "$GW:8539" "$GW:8549")

if [ "$STARTER_MODE" == "single" ]; then
  COORDINATORS=("$GW:8529")
fi

if [ "$SSL" == "true" ]; then
    STARTER_ARGS="$STARTER_ARGS --ssl.keyfile=/data/server.pem"
    SCHEME=https
    ARANGOSH_SCHEME=http+ssl
fi

if [ "$EXTENDED_NAMES" == "true" ]; then
    STARTER_ARGS="${STARTER_ARGS} --all.database.extended-names=true"
fi


docker stop adb

docker cp adb-data:/data - > result.tar
gzip result.tar

