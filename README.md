# PostgreSQL DB Extractor
[![GitHub Actions](https://github.com/keboola/db-extractor-pgsql/actions/workflows/push.yml/badge.svg)](https://github.com/keboola/db-extractor-pgsql/actions/workflows/push.yml)

This component extracts data from a PostgresSQL database.

## Example Configuration


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
          "primaryKey": null,
          "forceFallback": false, // use PDO export directly
          "useConsistentFallbackBooleanStyle": false // convert boolean values to t/f when using PDO statement
        }
      ]
    }

### Development

- Clone the repository.
- Create a `.env` file with `PGSQL_VERSION=latest`.
- Run `docker compose build`.

## License

MIT licensed, see the [LICENSE](./LICENSE) file.
