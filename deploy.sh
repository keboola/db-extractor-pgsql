#!/bin/bash
docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/db-extractor-pgsql quay.io/keboola/db-extractor-pgsql:$TRAVIS_TAG
docker images
docker push quay.io/keboola/db-extractor-pgsql:$TRAVIS_TAG
