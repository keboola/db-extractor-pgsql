docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD  quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository keboola keboola.ex-db-pgsql`
docker tag keboola/db-extractor-pgsql:latest $REPOSITORY:$TRAVIS_TAG
docker tag keboola/db-extractor-pgsql:latest $REPOSITORY:latest
eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login keboola keboola.ex-db-pgsql)
docker push $REPOSITORY:$TRAVIS_TAG
docker push $REPOSITORY:latest