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

## Incremental Fetching

Incremental fetching exports only rows whose value in `incrementalFetchingColumn` is greater than or
equal to the highest value seen on the previous run (the stored *watermark*). The column must be an
`INTEGER`, `NUMERIC`, `FLOAT` or `TIMESTAMP` column.

`incrementalFetchingMode` selects how the lower bound is computed. It is **optional and defaults to
`watermark`**, so existing configurations are unchanged.

- **`watermark`** (default) — `column >= last fetched value`.
  - `incrementalFetchingLookback` *(optional)* — re-fetch a margin *behind* the last value, so a row
    that was committed slightly after its timestamp (and therefore missed by a strict `>=`) is picked
    up on a later run. Give a **duration** for a `TIMESTAMP` column (e.g. `"20 minutes"`, `"1 hour"`)
    or a **number** for a numeric column (e.g. `"1000"`). The bound becomes `column >= (last value − N)`.
    A **primary key** is required (incremental loading deduplicates the re-fetched overlap).

- **`window`** — `column >= start [AND column <= end]`, **ignoring** the stored watermark. Useful for
  a bounded or segmented historical backfill.
  - `incrementalFetchingStart` — lower bound. For a `TIMESTAMP` column: relative (`"20 minutes ago"`,
    `"now"`) or absolute (`"2021-01-01"`); for a numeric column: a number. A primary key is required.
  - `incrementalFetchingEnd` *(optional)* — upper bound, same formats.

Example — watermark mode with a 20-minute lookback (the late-commit fix):

    {
      "incrementalFetchingColumn": "updated_at",
      "incrementalFetchingLookback": "20 minutes"
    }

Example — a bounded window (backfill):

    {
      "incrementalFetchingColumn": "updated_at",
      "incrementalFetchingMode": "window",
      "incrementalFetchingStart": "2021-01-01",
      "incrementalFetchingEnd": "2021-02-01"
    }

The modes are mutually exclusive; keys belonging to the other mode are ignored.

### Development

- Clone the repository.
- Create a `.env` file with `PGSQL_VERSION=latest`.
- Run `docker compose build`.

## License

MIT licensed, see the [LICENSE](./LICENSE) file.
