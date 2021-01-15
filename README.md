# PostgreSQL DB Extractor

[![Build Status](https://travis-ci.com/keboola/db-extractor-pgsql.svg?branch=master)](https://travis-ci.com/keboola/db-extractor-pgsql)


## Example configuration


    {
      "db": {
        "driver": "pgsql",
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "id": 1,
          "name": "employees",
          "query": "SELECT * FROM employees",
          "outputTable": "in.c-main.employees",
          "incremental": false,
          "enabled": true,
          "primaryKey": null
        }
      ]
    }

### Tests

The `ApplicationTest` tests are supposed to run versus an external db somewhere (rather than the pgsql image in the docker-compose)
They need the following environment variables set
```$xslt
EXTERNAL_PG_HOST=my.db.domain
EXTERNAL_PG_PORT=5432
EXTERNAL_PG_DATABASE=postgres
EXTERNAL_PG_USER=whoever
EXTERNAL_PG_PASSWORD=whoever's password
```
As per keboola convention, it is convenient to put this in a `.env` file and source it prior to running `docker-compose run --rm app`
