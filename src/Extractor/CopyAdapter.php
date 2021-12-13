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
use Symfony\Component\Process\Process;

class CopyAdapter implements ExportAdapter
{
    protected PgSQLDbConnection $connection;

    protected DatabaseConfig $databaseConfig;

    protected DefaultQueryFactory $queryFactory;

    protected PgSQLMetadataProvider $metadataProvider;

    public function __construct(
        PgSQLDbConnection $connection,
        DatabaseConfig $databaseConfig,
        DefaultQueryFactory $queryFactory,
        PgSQLMetadataProvider $metadataProvider
    ) {
        $this->connection = $connection;
        $this->databaseConfig = $databaseConfig;
        $this->queryFactory = $queryFactory;
        $this->metadataProvider = $metadataProvider;
    }

    public function testConnection(): void
    {
        try {
            $this->runCopyCommand('SELECT 1;', 30);
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

        try {
            return $this->doExport(
                $query,
                $exportConfig,
                $csvFilePath
            );
        } catch (CopyAdapterException $pdoError) {
            @unlink($csvFilePath);
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
        string $csvPath
    ): ExportResult {
        $sql =
            '\encoding UTF8;'.
            sprintf(
                $exportConfig->hasQuery() ? // include header?
                    "\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;" :
                    "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;",
                rtrim($query, '; '),
                $csvPath
            );

        try {
            $this->runCopyCommand($sql, null);
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }

        return $this->analyseOutput($csvPath, $exportConfig, $query);
    }

    protected function runCopyCommand(string $sql, ?float $timeout): void
    {
        $command = [];
        $command[] = sprintf('PGPASSWORD=%s', escapeshellarg($this->databaseConfig->getPassword()));
        $command[] = 'psql';
        $command[] = '-v ON_ERROR_STOP=1';
        $command[] = '-w';
        $command[] = escapeshellarg(PgSQLDsnFactory::createForCli($this->databaseConfig));

        $process = Process::fromShellCommandline(implode(' ', $command));
        $process->setInput($sql); // send SQL to STDIN
        $process->setTimeout($timeout); // null => allow it to run for as long as it needs
        $process->run();
        if ($process->getExitCode() !== 0) {
            throw new CopyAdapterException($process->getErrorOutput());
        }
    }

    private function analyseOutput(
        string $csvPath,
        ExportConfig $exportConfig,
        string $query
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
        if ($this->hasCsvHeader($exportConfig)) {
            $numRows--;
        }

        $incrementalLastFetchedValue = null;
        if ($exportConfig->isIncrementalFetching() && $lastFetchedRow) {
            $incrementalLastFetchedValue = $this->getLastFetchedValue(
                $exportConfig,
                $lastFetchedRow
            );
        }

        return new ExportResult(
            $csvPath,
            $numRows,
            $this->connection->createQueryMetadata($query),
            $this->hasCsvHeader($exportConfig),
            $incrementalLastFetchedValue
        );
    }

    private function getLastFetchedValue(
        ExportConfig $exportConfig,
        array $lastExportedRow
    ): string {
        $columns = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() :
            $this->metadataProvider->getTable($exportConfig->getTable())->getColumns()->getNames();

        $columnIndex = array_search($exportConfig->getIncrementalFetchingColumn(), $columns);
        if ($columnIndex === false) {
            throw new CopyAdapterException(sprintf(
                'The specified incremental fetching column %s not found in the table',
                $exportConfig->getIncrementalFetchingColumn()
            ));
        }

        return $lastExportedRow[$columnIndex];
    }

    private function hasCsvHeader(ExportConfig $exportConfig): bool
    {
        // If header is present in the CSV file, there are no columns metadata in the manifest.
        return $exportConfig->hasQuery();
    }
}
