<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

use Keboola\DbExtractor\TraitTests\DbConfigTrait;

trait ConfigTrait
{
    use DbConfigTrait;

    private function getConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "data_dir": "PGSQL",
    "tables": [
      {
        "id": 2,
        "name": "types",
        "outputTable": "in.c-main.types",
        "incremental": false,
        "primaryKey": ["character"],
        "enabled": true,
        "table": {
          "schema": "public",
          "tableName": "types"
        },
        "columns": ["character", "integer", "decimal", "boolean", "date"]
      },
      {
        "id": 2,
        "name": "escapingEmpty",
        "query": "SELECT * FROM escaping LIMIT 0",
        "outputTable": "in.c-main.escapingEmpty",
        "incremental": false,
        "primaryKey": [
          "orderId"
        ],
        "enabled": true
      },
      {
        "id": 3,
        "enabled": true,
        "name": "escaping",
        "outputTable": "in.c-main.escaping",
        "incremental": false,
        "primaryKey": null,
        "table": {
          "schema": "public",
          "tableName": "escaping"
        }
      }
    ]
  }
}
JSON;
        return json_decode(
            sprintf($configTemplate, json_encode($this->getDbConfigArray())),
            true
        );
    }

    public function getRowConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "data_dir": "PGSQL",
    "db": %s,
    "outputTable": "in.c-main.types",
    "table": {
      "schema": "public",
      "tableName": "types"
    },
    "primaryKey": ["character"],
    "retries": 3
  }
}
JSON;
        return json_decode(
            sprintf($configTemplate, json_encode($this->getDbConfigArray())),
            true
        );
    }


    public function getIncrementalConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "data_dir": "PGSQL",
    "db": %s,
    "table": {
        "tableName": "auto Increment Timestamp",
        "schema": "public"    
    },
    "name": "auto-increment-timestamp",
    "outputTable": "in.c-main.auto-increment-timestamp",
    "incremental": true,
    "primaryKey": ["_Weir%%d I-D"],
    "incrementalFetchingColumn": "_Weir%%d I-D",
    "retries": 3
  }
}
JSON;
        return json_decode(
            sprintf($configTemplate, json_encode($this->getDbConfigArray())),
            true
        );
    }
}
