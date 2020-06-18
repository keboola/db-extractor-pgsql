<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\CopyAdapterException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use PDOException;

class PgSQL extends Extractor
{
    public const DEFAULT_MAX_TRIES = 5;
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    private PgSQLMetadataProvider $metadataProvider;

    private PDOAdapter $pdoAdapter;

    private CopyAdapter $copyAdapter;

    private array $tablesToList = [];

    private bool $listColumns = true;

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

        $this->metadataProvider = new PgSQLMetadataProvider($this->logger, $this->pdoAdapter);
    }

    public function createConnection(array $dbParams): void
    {
        $this->pdoAdapter = new PDOAdapter($this->logger, $dbParams, $this->state);
        $this->copyAdapter = new CopyAdapter($this->logger, $dbParams, $this->state);
    }

    public function testConnection(): void
    {
        $this->pdoAdapter->testConnection();
        $this->copyAdapter->testConnection();
    }

    public function getTables(?array $tables = null): array
    {
        if (!$this->listColumns) {
            return $this->metadataProvider->getOnlyTables($this->tablesToList);
        }

        if ($this->tablesToList && !$tables) {
            $tables = $this->tablesToList;
        }

        return $this->metadataProvider->getTables($tables);
    }


    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $this->logger->info('Exporting to ' . $outputTable);

        if (!isset($table['query']) || $table['query'] === '') {
            $advancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $advancedQuery = true;
            $query = $table['query'];
        }

        $maxTries = isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES;
        $replaceBooleans = isset($table['useConsistentFallbackBooleanStyle']) ?
            $table['useConsistentFallbackBooleanStyle'] :
            false;
        $forceFallback = isset($table['forceFallback']) && $table['forceFallback'] === true;

        // Copy adapter
        $result = null;
        if ($forceFallback) {
            $this->logger->warning('Forcing extractor to use PDO fallback fetching');
        } else {
            $columnMetadata = $advancedQuery ? [] : $this->metadataProvider->getColumnMetadataFromTable($table);
            $this->logger->info(sprintf("Executing query '%s' via \copy ...", $outputTable));
            try {
                $result = $this->copyAdapter->export(
                    $query,
                    $advancedQuery,
                    $columnMetadata,
                    $this->incrementalFetching,
                    $this->createOutputCsv($outputTable)
                );
            } catch (CopyAdapterException $e) {
                @unlink($this->getOutputFilename($outputTable));
                $this->logger->info('Failed \copy command (will attempt via PDO): ' . $e->getMessage());
            }
        }

        // PDO (fallback) adapter
        if ($result === null) {
            $this->logger->info(sprintf("Executing query '%s' via PDO ...", $outputTable));
            try {
                $result = $this->pdoAdapter->export(
                    $query,
                    $advancedQuery,
                    $maxTries,
                    $replaceBooleans,
                    $this->incrementalFetching,
                    $this->createOutputCsv($outputTable)
                );
            } catch (PDOException $pdoError) {
                throw new UserException(
                    sprintf(
                        'Error executing [%s]: %s',
                        $outputTable,
                        $pdoError->getMessage()
                    )
                );
            }
        }

        if ($result['rows'] > 0) {
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
        } else {
            @unlink($this->getOutputFilename($outputTable));
            $this->logger->warning(
                sprintf(
                    'Query returned empty result. Nothing was imported to [%s]',
                    $table['outputTable']
                )
            );
        }

        // Output state
        $output = [
            'outputTable' => $outputTable,
            'rows' => $result['rows'],
        ];

        if (!empty($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }

        return $output;
    }


    public function simpleQuery(array $table, array $columns = []): string
    {
        $incrementalAddon = null;

        if (count($columns) > 0) {
            $query = sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column): string {
                    return $this->pdoAdapter->quoteIdentifier($column);
                }, $columns)),
                $this->pdoAdapter->quoteIdentifier($table['schema']),
                $this->pdoAdapter->quoteIdentifier($table['tableName'])
            );
        } else {
            $query = sprintf(
                'SELECT * FROM %s.%s',
                $this->pdoAdapter->quoteIdentifier($table['schema']),
                $this->pdoAdapter->quoteIdentifier($table['tableName'])
            );
        }

        if ($this->incrementalFetching && isset($this->incrementalFetching['column'])) {
            if (isset($this->state['lastFetchedRow'])) {
                $query .= sprintf(
                    ' WHERE %s >= %s',
                    $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']),
                    $this->pdoAdapter->quote((string) $this->state['lastFetchedRow'])
                );
            }
            $query .= sprintf(' ORDER BY %s', $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']));

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
            $this->pdoAdapter->quote($table['schema']),
            $this->pdoAdapter->quote($table['tableName']),
            $this->pdoAdapter->quote($columnName)
        );

        $columns = $this->pdoAdapter->runRetryableQuery($sql);
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

    public function getMaxOfIncrementalFetchingColumn(array $table): ?string
    {
        $result = $this->pdoAdapter->runRetryableQuery(sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']),
            $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']),
            $this->pdoAdapter->quoteIdentifier($table['schema']),
            $this->pdoAdapter->quoteIdentifier($table['tableName'])
        ));

        return count($result) > 0 ? $result[0][$this->incrementalFetching['column']] : null;
    }
}
