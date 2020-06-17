<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use PDOException;
use Psr\Log\LoggerInterface;
use InvalidArgumentException;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\CopyAdapterException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;

class PgSQL extends BaseExtractor
{
    public const INCREMENTAL_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT', 'TIMESTAMP'];

    private PDOAdapter $pdoAdapter;

    private CopyAdapter $copyAdapter;

    private ?MetadataProvider $metadataProvider = null;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $parameters['db']['ssh']['compression'] = true;
        parent::__construct($parameters, $state, $logger);
    }

    public function getMetadataProvider(): MetadataProvider
    {
        if (!$this->metadataProvider) {
            $this->metadataProvider = new PgSQLMetadataProvider($this->logger, $this->pdoAdapter);
        }
        return $this->metadataProvider;
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

    public function export(ExportConfig $exportConfig): array
    {
        if (!$exportConfig instanceof PgsqlExportConfig) {
            throw new InvalidArgumentException('PgsqlExportConfig expected.');
        }

        if ($exportConfig->isIncrementalFetching()) {
            $this->validateIncrementalFetching($exportConfig);
        }

        $this->logger->info('Exporting to ' . $exportConfig->getOutputTable());
        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->simpleQuery($exportConfig);
        $logPrefix = $exportConfig->hasConfigName() ? $exportConfig->getConfigName() : $exportConfig->getOutputTable();

        // Copy adapter
        $result = null;
        if ($exportConfig->getForceFallback()) {
            $this->logger->warning('Forcing extractor to use PDO fallback fetching');
        } else {
            $this->logger->info(sprintf("Executing query '%s' via \copy ...", $logPrefix));
            try {
                $result = $this->copyAdapter->export(
                    $query,
                    $exportConfig,
                    $this->getMetadataProvider(),
                    $this->getOutputFilename($exportConfig->getOutputTable())
                );
            } catch (CopyAdapterException $e) {
                @unlink($this->getOutputFilename($exportConfig->getOutputTable()));
                $this->logger->info('Failed \copy command (will attempt via PDO): ' . $e->getMessage());
            }
        }

        // PDO (fallback) adapter
        if ($result === null) {
            $this->logger->info(sprintf("Executing query '%s' via PDO ...", $logPrefix));
            try {
                $result = $this->pdoAdapter->export(
                    $query,
                    $exportConfig,
                    $this->createOutputCsv($exportConfig->getOutputTable())
                );
            } catch (PDOException $pdoError) {
                throw new UserException(
                    sprintf('Error executing "%s": %s', $logPrefix, $pdoError->getMessage())
                );
            }
        }

        // Manifest
        if ($result['rows'] > 0) {
            $this->createManifest($exportConfig);
        } else {
            @unlink($this->getOutputFilename($exportConfig->getOutputTable()));
            $this->logger->warning(sprintf(
                'Query returned empty result. Nothing was imported to "%s"',
                $exportConfig->getOutputTable(),
            ));
        }

        // Output state
        $output = [
            'outputTable' => $exportConfig->getOutputTable(),
            'rows' => $result['rows'],
        ];

        if (!empty($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }

        return $output;
    }

    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $sql = [];

        if ($exportConfig->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->pdoAdapter->quoteIdentifier($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->pdoAdapter->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->pdoAdapter->quoteIdentifier($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            $sql[] = sprintf(
                // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $this->pdoAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $this->pdoAdapter->quote((string) $this->state['lastFetchedRow'])
            );
        }

        if ($exportConfig->isIncrementalFetching()) {
            $sql[] = sprintf(
                'ORDER BY %s',
                $this->pdoAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn())
            );
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf('LIMIT %d', $exportConfig->getIncrementalFetchingLimit());
        }

        return implode(' ', $sql);
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this
                ->getMetadataProvider()
                ->getTable($exportConfig->getTable())
                ->getColumns()
                ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        try {
            $datatype = new GenericStorage($column->getType());
            $type = $datatype->getBasetype();
        } catch (InvalidLengthException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching must has numeric or timestamp type.',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        if (!in_array($type, self::INCREMENTAL_TYPES, true)) {
            throw new UserException(sprintf(
                'Column "%s" specified for incremental fetching has unexpected type "%s", expected: "%s".',
                $exportConfig->getIncrementalFetchingColumn(),
                $datatype->getBasetype(),
                implode('", "', self::INCREMENTAL_TYPES),
            ));
        }
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $result = $this->pdoAdapter->runRetryableQuery(sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->pdoAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->pdoAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->pdoAdapter->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->pdoAdapter->quoteIdentifier($exportConfig->getTable()->getName())
        ));

        return count($result) > 0 ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }
}
