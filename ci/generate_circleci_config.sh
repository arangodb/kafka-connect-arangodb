#!/usr/bin/env bash
set -o errexit -o pipefail

# Getting a list of releases from the release tracker
versions_json=$(curl -X 'GET' "https://$RELEASE_TRACKER_USERNAME:$RELEASE_TRACKER_PASSWORD@release-tracker.arangodb.com/list-releases-for-stable-branches?os=linux&arch=x86_64" -H 'accept: application/json')

# Filter out the latest release for each stable branch
latest_versions=$(echo "$versions_json" | jq -r '[keys[] as $k | "\(.[$k] | .[-1])"]')

echo "Installing yq..."
sudo snap install yq

# We use yq to replace the target-version key, this is the only edit in place we need
yq -i ".workflows.test-distributed-arangodb-versions.jobs[0].test-distributed.matrix.parameters.arango-version = $latest_versions" .circleci/continue_config.yml