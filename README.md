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

### Development

- Clone the repository.
- Create `.env` file with `PHSQL_VERSION=latest`.
- Run `docker-compose build`.
