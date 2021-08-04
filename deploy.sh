#!/usr/bin/env bash

set -e

GITHUB_TAG=${GITHUB_REF/refs\/tags\//}

docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository keboola keboola.ex-db-pgsql`
docker tag ${APP_IMAGE}:latest $REPOSITORY:${GITHUB_TAG}
docker tag ${APP_IMAGE}:latest $REPOSITORY:latest
eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login keboola keboola.ex-db-pgsql)
docker push $REPOSITORY:${GITHUB_TAG}
docker push $REPOSITORY:latest

docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  quay.io/keboola/developer-portal-cli-v2:latest update-app-repository keboola keboola.ex-db-pgsql ${GITHUB_TAG}
