<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;
use Keboola\DbExtractorLogger\Logger;

class PgSQLMetadataProvider
{
    private Logger $logger;

    private PDOAdapter $pdoAdapter;

    public function __construct(Logger $logger, PDOAdapter $pdoAdapter)
    {
        $this->logger = $logger;
        $this->pdoAdapter = $pdoAdapter;
    }

    public function getOnlyTables(array $tables = []): array
    {
        $sql = <<<EOT
    SELECT 
      ns.nspname AS table_schema,
      c.relname AS table_name,
      c.relkind AS table_type
    FROM pg_class c 
    INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace --schemas    
    WHERE c.relkind IN ('r', 'S', 't', 'v', 'm', 'f', 'p') 
      AND ns.nspname != 'information_schema' -- exclude system namespaces
      AND ns.nspname != 'pg_catalog' 
      AND ns.nspname NOT LIKE 'pg_toast%'
      AND ns.nspname NOT LIKE 'pg_temp%'
EOT;
        if ($tables) {
            $sql .= sprintf(
                ' AND c.relname IN (%s) AND ns.nspname IN (%s)',
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->pdoAdapter->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->pdoAdapter->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }
        $resultArray = $this->pdoAdapter->runRetryableQuery($sql);
        $tableDefs = [];
        foreach ($resultArray as $table) {
            $tableDefs[$table['table_schema'] . '.' . $table['table_name']] = [
                'name' => $table['table_name'],
                'schema' => $table['table_schema'] ?? null,
                'type' => $this->tableTypeFromCode($table['table_type']),
            ];
        }
        ksort($tableDefs);
        return array_values($tableDefs);
    }


    public function getTables(?array $tables = null): array
    {
        $version = $this->getDbServerVersion();
        $defaultValueStatement = $version < 120000 ? 'd.adsrc' : 'pg_get_expr(d.adbin::pg_node_tree, d.adrelid)';

        $sql = <<<EOT
    SELECT 
      ns.nspname AS table_schema,
      c.relname AS table_name,
      c.relkind AS table_type,
      a.attname AS column_name,
      format_type(a.atttypid, a.atttypmod) AS data_type_with_length,
      NOT a.attnotnull AS nullable,
      i.indisprimary AS primary_key,
      a.attnum AS ordinal_position,
      $defaultValueStatement AS default_value
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid AND c.reltype != 0 --indexes have 0 reltype, we don't want them here
    INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace --schemas
    LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) -- PKs
    LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid,  d.adnum) -- default values
    WHERE 
      NOT a.attisdropped -- exclude dropped columns
      AND a.attnum > 0 -- exclude system columns
      AND ns.nspname != 'information_schema' -- exclude system namespaces
      AND ns.nspname != 'pg_catalog' 
      AND ns.nspname NOT LIKE 'pg_toast%'
      AND ns.nspname NOT LIKE 'pg_temp%'
EOT;

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                ' AND c.relname IN (%s) AND ns.nspname IN (%s)',
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->pdoAdapter->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->pdoAdapter->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }

        $resultArray = $this->pdoAdapter->runRetryableQuery($sql);

        if (empty($resultArray)) {
            return [];
        }
        $tableDefs = [];
        foreach ($resultArray as $column) {
            $curTable = $column['table_schema'] . '.' . $column['table_name'];
            if (!array_key_exists($curTable, $tableDefs)) {
                $table = new Table();
                $table
                    ->setName((string) $column['table_name'])
                    ->setSchema($column['table_schema'] ?? '')
                    ->setType((string) $this->tableTypeFromCode($column['table_type']));

                $tableDefs[$curTable] = $table;
            } else {
                $table = $tableDefs[$curTable];
            }

            $dataType = $column['data_type_with_length'];
            $ret = preg_match('/(.*)\((\d+|\d+,\d+)\)/', $dataType, $parsedType);
            $length = null;
            if ($ret === 1) {
                $dataType = isset($parsedType[1]) ? $parsedType[1] : null;
                $length = isset($parsedType[2]) ? $parsedType[2] : null;
            }

            $default = $column['default_value'];
            if ($dataType === 'character varying' && $default !== null) {
                $default = str_replace("'", '', explode('::', $column['default_value'])[0]);
            }
            $tableColumn = new TableColumn();
            $tableColumn
                ->setName($column['column_name'])
                ->setType($dataType)
                ->setPrimaryKey($column['primary_key'] ?: false)
                ->setLength($length)
                ->setNullable($column['nullable'])
                ->setDefault($default)
                ->setOrdinalPosition($column['ordinal_position']);

            $table->addColumn($tableColumn);
        }
        array_walk($tableDefs, function (Table &$item): void {
            $item = $item->getOutput();
        });
        foreach ($tableDefs as $tableId => $tableData) {
            /**
             * @var mixed $a
             * @var mixed $b
             * @return int
             */
            usort($tableData['columns'], function ($a, $b) {
                return (int) ($a['ordinalPosition'] > $b['ordinalPosition']);
            });
            $tableDefs[$tableId]['columns'] = array_values($tableData['columns']);
        }
        ksort($tableDefs);
        return array_values($tableDefs);
    }

    public function getColumnMetadataFromTable(array $table): array
    {
        $columns = $table['columns'];
        $tableMetadata = $this->getTables([$table['table']]);
        if (count($tableMetadata) === 0) {
            throw new UserException(sprintf(
                'Could not find the table: [%s].[%s]',
                $table['table']['schema'],
                $table['table']['tableName']
            ));
        }
        $tableMetadata = $tableMetadata[0];
        $columnMetadata = $tableMetadata['columns'];
        // if columns are selected
        if (count($columns) > 0) {
            $columnMetadata = array_filter($columnMetadata, function ($columnMeta) use ($columns) {
                return in_array($columnMeta['name'], $columns);
            });
            $colOrder = array_flip($columns);
            usort($columnMetadata, function (array $colA, array $colB) use ($colOrder) {
                return $colOrder[$colA['name']] - $colOrder[$colB['name']];
            });
        }
        return $columnMetadata;
    }

    private function tableTypeFromCode(string $code): ?string
    {
        switch ($code) {
            case 'r':
                return 'table';
            case 'v':
                return 'view';
            case 'm':
                return 'materialized view';
            case 'f':
                return 'foreign table';
            case 'i':
                return 'index';
            case 'S':
                return 'sequence';
            case 'c':
                return 'composite type';
            case 't':
                return 'toast table';
            default:
                return null;
        }
    }

    private function getDbServerVersion(): int
    {
        $sqlGetVersion = 'SHOW server_version_num;';
        $version = $this->pdoAdapter->runRetryableQuery($sqlGetVersion);
        $this->logger->info(
            sprintf('Found database server version: %s', $version[0]['server_version_num'])
        );
        return (int) $version[0]['server_version_num'];
    }
}
