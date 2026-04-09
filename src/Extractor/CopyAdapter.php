<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\Exception\CopyAdapterConnectionException;
use Keboola\DbExtractor\Exception\CopyAdapterException;
use Keboola\DbExtractor\Exception\CopyAdapterQueryException;
use Keboola\DbExtractor\Exception\CopyAdapterSkippedException;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Symfony\Component\Process\Process;

class CopyAdapter implements ExportAdapter
{
    protected PgSQLDbConnection $connection;

    protected DatabaseConfig $databaseConfig;

    protected DefaultQueryFactory $queryFactory;

    protected PgSQLMetadataProvider $metadataProvider;

    protected LoggerInterface $logger;

    public function __construct(
        PgSQLDbConnection $connection,
        DatabaseConfig $databaseConfig,
        DefaultQueryFactory $queryFactory,
        PgSQLMetadataProvider $metadataProvider,
        LoggerInterface $logger,
    ) {
        $this->connection = $connection;
        $this->databaseConfig = $databaseConfig;
        $this->queryFactory = $queryFactory;
        $this->metadataProvider = $metadataProvider;
        $this->logger = $logger;
    }

    public function testConnection(): void
    {
        try {
            $this->runPsqlProcess('SELECT 1;', 30);
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterConnectionException('Failed psql connection: ' . $e->getMessage());
        }
    }

    public function getName(): string
    {
        return 'Copy';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!$exportConfig instanceof PgsqlExportConfig) {
            throw new InvalidArgumentException('PgsqlExportConfig expected.');
        }

        if ($exportConfig->getForceFallback()) {
            throw new CopyAdapterSkippedException('Disabled in configuration.');
        }

        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);

        $maxAttempts = $exportConfig->isCopyAdapterRetriesEnabled() ? $exportConfig->getMaxRetries() : 1;
        $proxy = new RetryProxy(
            new SimpleRetryPolicy($maxAttempts, [CopyAdapterQueryException::class]),
            new ExponentialBackOffPolicy(1000),
            $this->logger,
        );

        try {
            return $proxy->call(function () use ($query, $exportConfig, $csvFilePath): ExportResult {
                try {
                    return $this->doExport($query, $exportConfig, $csvFilePath);
                } catch (CopyAdapterQueryException $e) {
                    @unlink($csvFilePath);
                    throw $e;
                }
            });
        } catch (CopyAdapterException $pdoError) {
            @unlink($csvFilePath);
            $this->logger->info(sprintf('Copy adapter failed, falling back to PDO: %s', $pdoError->getMessage()));
            throw new UserException($pdoError->getMessage());
        }
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this->queryFactory->create($exportConfig, $this->connection);
    }

    public function doExport(
        string $query,
        PgsqlExportConfig $exportConfig,
        string $csvPath,
    ): ExportResult {
        $trimmedQuery = rtrim($query, '; ');
        $sql = '\encoding UTF8' . PHP_EOL .
            implode(PHP_EOL, $this->databaseConfig->getInitQueries()) . PHP_EOL;

        $canCreateView = !$exportConfig->getEnforceCopy() && $this->canUserCreateView();
        $isReadOnly = $this->isTransactionReadOnly();
        if ($canCreateView && !$isReadOnly) {
            $viewName = uniqid();
            $sql .= 'CREATE TEMP VIEW "' . $viewName . '" AS ' . $trimmedQuery . ';' . PHP_EOL .
                sprintf(
                    "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;",
                    'SELECT * FROM "' . $viewName . '"',
                    $csvPath,
                ) . PHP_EOL .
                'DROP VIEW "' . $viewName . '";';
        } else {
            if ($exportConfig->getEnforceCopy()) {
                $this->logger->info('Using direct COPY (enforceCopy is enabled).');
            } elseif (!$canCreateView) {
                $this->logger->info('Using direct COPY (no CREATE privilege on current schema).');
            } elseif ($isReadOnly) {
                $this->logger->info('Using direct COPY (transaction is read-only).');
            }
            $sql .= sprintf(
                "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;",
                $trimmedQuery,
                $csvPath,
            );
        }

        try {
            $this->runPsqlProcess($sql, null);
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }

        return $this->analyseOutput($csvPath, $exportConfig, $query);
    }

    protected function runPsqlProcess(string $sql, ?float $timeout): string
    {
        $this->logger->debug(sprintf('Running query: "%s".', $sql));
        $command = [];
        $command[] = sprintf('PGPASSWORD=%s', escapeshellarg($this->databaseConfig->getPassword()));
        $command[] = 'psql';
        $command[] = '-v ON_ERROR_STOP=1';
        $command[] = '-w';
        $command[] = '-t';
        $command[] = escapeshellarg(PgSQLDsnFactory::createForCli($this->databaseConfig));

        $process = Process::fromShellCommandline(implode(' ', $command));
        $process->setInput($sql); // send SQL to STDIN
        $process->setTimeout($timeout); // null => allow it to run for as long as it needs
        $process->run();
        $exitCode = $process->getExitCode();
        $this->logger->debug(sprintf(
            'psql process finished with exit code %d. Stderr: "%s". Stdout: "%s".',
            $exitCode,
            trim($process->getErrorOutput()),
            trim($process->getOutput()),
        ));
        if ($exitCode !== 0) {
            $errorMsg = trim($process->getErrorOutput());
            if ($errorMsg === '') {
                $errorMsg = trim($process->getOutput());
            }
            if ($errorMsg === '') {
                $errorMsg = sprintf('psql process failed with exit code %d', $exitCode);
            }
            throw new CopyAdapterException($errorMsg);
        }

        return trim($process->getOutput());
    }

    private function analyseOutput(
        string $csvPath,
        ExportConfig $exportConfig,
        string $query,
    ): ExportResult {
        // Get the number of written rows and lastFetchedValue
        $numRows = 0;
        $lastFetchedRow = null;
        $reader = new CsvReader($csvPath);
        $colCount = $reader->getColumnsCount();
        while ($reader->valid()) {
            if (count($reader->current()) !== $colCount) {
                throw new CopyAdapterException('The \copy command produced an invalid csv.');
            }
            $lastRow = $reader->current();
            $reader->next();
            if (!$reader->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }

        $incrementalLastFetchedValue = null;
        if ($exportConfig->isIncrementalFetching() && $lastFetchedRow) {
            $incrementalLastFetchedValue = $this->getLastFetchedValue(
                $exportConfig,
                $lastFetchedRow,
            );
        }

        return new ExportResult(
            $csvPath,
            $numRows,
            new CopyAdapterQueryMetadata($this->connection, $query),
            false,
            $incrementalLastFetchedValue,
        );
    }

    private function getLastFetchedValue(
        ExportConfig $exportConfig,
        array $lastExportedRow,
    ): string {
        $columns = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() :
            $this->metadataProvider->getTable($exportConfig->getTable())->getColumns()->getNames();

        $columnIndex = array_search($exportConfig->getIncrementalFetchingColumn(), $columns);
        if ($columnIndex === false) {
            throw new CopyAdapterException(sprintf(
                'The specified incremental fetching column %s not found in the table',
                $exportConfig->getIncrementalFetchingColumn(),
            ));
        }

        return $lastExportedRow[$columnIndex];
    }

    protected function canUserCreateView(): bool
    {
        try {
            return $this->runPsqlProcess(
                'SELECT has_schema_privilege(current_schema(), \'CREATE\');',
                null,
            ) === 't';
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }
    }

    protected function isTransactionReadOnly(): bool
    {
        try {
            return $this->runPsqlProcess(
                'SHOW transaction_read_only;',
                null,
            ) === 'on';
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }
    }
}
