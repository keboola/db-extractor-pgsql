#!/usr/bin/env bash

# install dependencies
composer install -n;

# load data to database
yum install -y postgresql;
psql -h pgsql -U postgres -d postgres -c "DROP TABLE IF EXISTS escaping;";
psql -h pgsql -U postgres -d postgres -c "CREATE TABLE escaping (col1 VARCHAR NOT NULL, col2 VARCHAR NOT NULL);";
psql -h pgsql -U postgres -d postgres -c "\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV HEADER;";

# run test suite
export ROOT_PATH="/code";
./vendor/bin/phpunit;
