<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\Utils;
use Symfony\Component\Process\Process;
use PDO;
use PDOException;
use Throwable;

class PgSQL extends Extractor
{
    public const DEFAULT_MAX_TRIES = 5;
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];


    /** @var  array */
    private $dbConfig;

    /** @var array */
    private $tablesToList = [];

    /** @var bool */
    private $listColumns = true;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        $parameters['db']['ssh']['compression'] = true;
        parent::__construct($parameters, $state, $logger);
        if (!empty($parameters['tableListFilter'])) {
            if (!empty($parameters['tableListFilter']['tablesToList'])) {
                $this->tablesToList = $parameters['tableListFilter']['tablesToList'];
            }
            if (isset($parameters['tableListFilter']['listColumns'])) {
                $this->listColumns = $parameters['tableListFilter']['listColumns'];
            }
        }
    }

    public function createConnection(array $dbParams): PDO
    {
        $this->dbConfig = $dbParams;

        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 60,
        ];

        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5432';

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            $dbParams['host'],
            $port,
            $dbParams['database']
        );
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        $pdo = new PDO($dsn, $dbParams['user'], $dbParams['#password'], $options);
        $pdo->exec("SET NAMES 'UTF8';");

        return $pdo;
    }

    private function getColumnMetadataFromTable(array $table): array
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

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];

        $this->logger->info('Exporting to ' . $outputTable);
        $advancedQuery = true;
        $columnMetadata = [];
        if (!isset($table['query']) || $table['query'] === '') {
            $columnMetadata = $this->getColumnMetadataFromTable($table);
            $query = $this->simpleQuery($table['table'], $columnMetadata);
            $advancedQuery = false;
        } else {
            $query = $table['query'];
        }

        $maxTries = isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES;
        $exception = null;

        try {
            if (isset($table['forceFallback']) && $table['forceFallback'] === true) {
                throw new \Exception('Forcing extractor to use PDO fallback fetching');
            }
            $result = $this->executeCopyQuery(
                $query,
                $this->createOutputCsv($outputTable),
                $table['outputTable'],
                $advancedQuery
            );
            if (!$advancedQuery && isset($result['lastFetchedRow'])) {
                $result['lastFetchedRow'] = $this->getLastFetchedValue($columnMetadata, $result['lastFetchedRow']);
            }
        } catch (Throwable $copyError) {
            // There was an error, so let's try the old method
            if (!$copyError instanceof ApplicationException) {
                if (isset($table['forceFallback']) && $table['forceFallback'] === true) {
                    $this->logger->warning($copyError->getMessage());
                } else {
                    $this->logger->warning('Unexpected exception executing \copy: ' . $copyError->getMessage());
                }
            }
            try {
                // recreate the db connection
                $this->tryReconnect();
            } catch (Throwable $connectionError) {
            };
            $proxy = new DbRetryProxy($this->logger, $maxTries);
            try {
                $result = $proxy->call(function () use ($query, $outputTable, $advancedQuery) {
                    try {
                        return $this->executeQueryPDO($query, $this->createOutputCsv($outputTable), $advancedQuery);
                    } catch (Throwable $queryError) {
                        try {
                            $this->db = $this->createConnection($this->getDbParameters());
                        } catch (Throwable $connectionError) {
                        };
                        throw $queryError;
                    }
                });
            } catch (PDOException $pdoError) {
                throw new UserException(
                    sprintf(
                        'Error executing [%s]: %s',
                        $table['outputTable'],
                        $pdoError->getMessage()
                    )
                );
            } catch (Throwable $generalError) {
                throw($generalError);
            }
        }
        if ($this->createManifest($table) === false) {
            throw new ApplicationException(
                'Unable to create manifest',
                0,
                null,
                [
                'table' => $table,
                ]
            );
        }

        $output = [
            'outputTable'=> $outputTable,
            'rows' => $result['rows'],
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }
        return $output;
    }

    protected function executeQueryPDO(string $query, CsvFile $csv, bool $advancedQuery): array
    {
        $cursorName = 'exdbcursor' . intval(microtime(true));
        $curSql = "DECLARE $cursorName CURSOR FOR $query";
        $this->logger->info('Executing query via PDO ...');
        try {
            $this->db->beginTransaction(); // cursors require a transaction.
            $stmt = $this->db->prepare($curSql);
            $stmt->execute();
            $innerStatement = $this->db->prepare("FETCH 1 FROM $cursorName");
            $innerStatement->execute();
            // write header and first line
            $resultRow = $innerStatement->fetch(PDO::FETCH_ASSOC);
            if (!is_array($resultRow) || empty($resultRow)) {
                $this->logger->warning('Query returned empty result. Nothing was imported');
                // no rows found.  If incremental fetching is turned on, we need to preserve the last state
                if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
                    $output['lastFetchedRow'] = $this->state['lastFetchedRow'];
                }
                $output['rows'] = 0;
                return $output;
            }
            // only write header for advanced query case
            if ($advancedQuery) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            $numRows = 1;
            $lastRow = $resultRow;
            // write the rest
            $this->logger->info('Fetching data...');
            $innerStatement = $this->db->prepare("FETCH 10000 FROM $cursorName");
            while ($innerStatement->execute() && count($resultRows = $innerStatement->fetchAll(PDO::FETCH_ASSOC)) > 0) {
                foreach ($resultRows as $resultRow) {
                    $csv->writeRow($resultRow);
                    $lastRow = $resultRow;
                    $numRows++;
                }
            }
            // close the cursor
            $this->db->exec("CLOSE $cursorName");
            $this->db->commit();

            // get last fetched value
            if (isset($this->incrementalFetching['column'])) {
                if (!array_key_exists($this->incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $this->incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$this->incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            $this->logger->info("Extraction completed. Fetched {$numRows} rows.");
        } catch (PDOException $e) {
            try {
                $this->db->rollBack();
            } catch (Throwable $e2) {
            }
            $innerStatement = null;
            $stmt = null;
            throw $e;
        }
        return $output;
    }

    protected function executeCopyQuery(string $query, CsvFile $csvFile, string $tableName, bool $advancedQuery): array
    {
        $this->logger->info(sprintf("Executing query '%s' via \copy ...", $tableName));

        $copyCommand = "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;";
        if ($advancedQuery) {
            $copyCommand = "\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;";
        }

        $command = sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c %s",
            $this->dbConfig['#password'],
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['user'],
            $this->dbConfig['database'],
            escapeshellarg(
                sprintf(
                    $copyCommand,
                    rtrim($query, '; '),
                    $csvFile
                )
            )
        );

        $process = Process::fromShellCommandline($command);
        // allow it to run for as long as it needs
        $process->setTimeout(null);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->logger->info('Failed \copy command (will attempt via PDO): ' . $process->getErrorOutput());
            throw new ApplicationException('Error using copy command.', 42);
        }
        return $this->analyseOutput($csvFile);
    }

    private function analyseOutput(CsvFile $csvFile): array
    {
        // Get the number of written rows and lastFetchedValue
        $outputFile = $csvFile;
        $numRows = 0;
        $lastFetchedRow = null;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                throw new UserException('The \copy command produced an invalid csv.');
            }
            $lastRow = $outputFile->current();
            $outputFile->next();
            if (!$outputFile->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }
        $this->logger->info(sprintf('Successfully exported %d rows.', $numRows));
        $output = ['rows' => $numRows];
        if ($lastFetchedRow && isset($this->incrementalFetching['column'])) {
            $output['lastFetchedRow'] = $lastFetchedRow;
        }
        return $output;
    }

    private function getLastFetchedValue(array $columnMetadata, array $lastExportedRow): string
    {
        foreach ($columnMetadata as $key => $column) {
            if ($column['name'] === $this->incrementalFetching['column']) {
                return $lastExportedRow[$key];
            }
        }
        throw new UserException(
            sprintf(
                'The specified incremental fetching column %s not found in the table',
                $this->incrementalFetching['column']
            )
        );
    }

    public function testConnection(): void
    {
        // check PDO connection
        $this->db->query('SELECT 1');

        // check psql connection
        $command = sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"SELECT 1;\"",
            $this->dbConfig['#password'],
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['user'],
            $this->dbConfig['database']
        );
        $process = Process::fromShellCommandline($command);
        $process->run();
        if ($process->getExitCode() !== 0) {
            throw new UserException('Failed psql connection: ' . $process->getErrorOutput());
        }
    }

    private function getOnlyTables(array $tables = []): array
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
                            return $this->db->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }
        $resultArray = $this->runRetriableQuery($sql);
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
        if (!$this->listColumns) {
            return $this->getOnlyTables($this->tablesToList);
        }
        if ($this->tablesToList && !$tables) {
            $tables = $this->tablesToList;
        }

        $version = $this->getDbServerVersion();
        $defaultValueStatement = 'pg_get_expr(d.adbin::pg_node_tree, d.adrelid)';
        if ($version < 120000) {
            $defaultValueStatement = 'd.adsrc';
        }

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
                            return $this->db->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }

        $resultArray = $this->runRetriableQuery($sql);

        if (empty($resultArray)) {
            return [];
        }
        $tableDefs = [];
        foreach ($resultArray as $column) {
            $curTable = $column['table_schema'] . '.' . $column['table_name'];
            if (!array_key_exists($curTable, $tableDefs)) {
                $tableDefs[$curTable] = [
                    'name' => $column['table_name'],
                    'schema' => $column['table_schema'] ?? null,
                    'type' => $this->tableTypeFromCode($column['table_type']),
                    'columns' => [],
                ];
            }

            $ret = preg_match('/(.*)\((\d+|\d+,\d+)\)/', $column['data_type_with_length'], $parsedType);

            $data_type = $column['data_type_with_length'];
            $length = null;
            if ($ret === 1) {
                $data_type = isset($parsedType[1]) ? $parsedType[1] : null;
                $length = isset($parsedType[2]) ? $parsedType[2] : null;
            }

            $default = $column['default_value'];
            if ($data_type === 'character varying' && $default !== null) {
                $default = str_replace("'", '', explode('::', $column['default_value'])[0]);
            }
            $tableDefs[$curTable]['columns'][$column['ordinal_position'] - 1] = [
                'name' => $column['column_name'],
                'sanitizedName' => Utils\sanitizeColumnName($column['column_name']),
                'type' => $data_type,
                'primaryKey' => $column['primary_key'] ?: false,
                'length' => $length,
                'nullable' => $column['nullable'],
                'default' => $default,
                'ordinalPosition' => $column['ordinal_position'],
            ];

            // make sure columns are sorted by index which is ordinal_position - 1
            ksort($tableDefs[$curTable]['columns']);
        }
        foreach ($tableDefs as $tableId => $tableData) {
            $tableDefs[$tableId]['columns'] = array_values($tableData['columns']);
        }
        ksort($tableDefs);
        return array_values($tableDefs);
    }

    private function getDbServerVersion(): int
    {
        $sqlGetVersion = 'SHOW server_version_num;';
        $version = $this->runRetriableQuery($sqlGetVersion);
        $this->logger->info(
            sprintf('Found database server version: %s', $version[0]['server_version_num'])
        );
        return (int) $version[0]['server_version_num'];
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

    public function simpleQuery(array $table, array $columns = []): string
    {
        $incrementalAddon = null;

        if (count($columns) > 0) {
            $query = sprintf(
                'SELECT %s FROM %s.%s',
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            return $this->quote($column['name']);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            $query = sprintf(
                'SELECT * FROM %s.%s',
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }

        if ($this->incrementalFetching && isset($this->incrementalFetching['column'])) {
            if (isset($this->state['lastFetchedRow'])) {
                $query .= sprintf(
                    ' WHERE %s >= %s',
                    $this->quote($this->incrementalFetching['column']),
                    $this->db->quote((string) $this->state['lastFetchedRow'])
                );
            }
            $query .= sprintf(' ORDER BY %s', $this->quote($this->incrementalFetching['column']));

            if (isset($this->incrementalFetching['limit'])) {
                $query .= sprintf(
                    ' LIMIT %d',
                    $this->incrementalFetching['limit']
                );
            }
        }
        return $query;
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $sql = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
            $this->db->quote($table['schema']),
            $this->db->quote($table['tableName']),
            $this->db->quote($columnName)
        );

        $columns = $this->runRetriableQuery($sql);
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }

        try {
            $datatype = new GenericStorage($columns[0]['data_type']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (\Keboola\Datatype\Definition\Exception\InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $columnName
                )
            );
        }

        if ($limit && $limit >= 0) {
            $this->incrementalFetching['limit'] = $limit;
        } else if ($limit < 0) {
            throw new UserException('The limit parameter must be an integer >= 0');
        }
    }

    private function runRetriableQuery(string $query, array $values = []): array
    {
        $retryProxy = new DbRetryProxy($this->logger);
        return $retryProxy->call(function () use ($query, $values): array {
            try {
                $stmt = $this->db->prepare($query);
                $stmt->execute($values);
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            } catch (\Throwable $exception) {
                $this->tryReconnect();
                throw $exception;
            }
        });
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $deadConnectionException) {
            $reconnectionRetryProxy = new DbRetryProxy(
                $this->logger,
                self::DEFAULT_MAX_TRIES,
                null,
                1000
            );
            try {
                $this->db = $reconnectionRetryProxy->call(function () {
                    return $this->createConnection($this->getDbParameters());
                });
            } catch (\Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    $reconnectException->getCode(),
                    $reconnectException
                );
            }
        }
    }

    private function quote(string $obj): string
    {
        return "\"{$obj}\"";
    }
}
