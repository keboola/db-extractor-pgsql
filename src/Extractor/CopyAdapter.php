<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\Exception\CopyAdapterConnectionException;
use Keboola\DbExtractor\Exception\CopyAdapterException;
use Keboola\DbExtractor\Exception\CopyAdapterQueryException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

class CopyAdapter
{
    private LoggerInterface $logger;

    private array $dbParams;

    private array $state;

    public function __construct(LoggerInterface $logger, array $dbParams, array $state)
    {
        $this->logger = $logger;
        $this->dbParams = $dbParams;
        $this->state = $state;
    }

    public function testConnection(): void
    {
        try {
            $this->runQuery('SELECT 1;');
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterConnectionException('Failed psql connection: ' . $e->getMessage());
        }
    }

    public function export(
        string $query,
        PgsqlExportConfig $exportConfig,
        MetadataProvider $metadataProvider,
        string $csvPath
    ): array {
        $result = $this->runCopyQuery($query, $exportConfig, $csvPath);
        if (!$exportConfig->hasQuery() && isset($result['lastFetchedRow'])) {
            $result['lastFetchedRow'] = $this->getLastFetchedValue(
                $exportConfig,
                $metadataProvider,
                $result['lastFetchedRow']
            );
        }
        return $result;
    }

    protected function runQuery(string $sql): void
    {
        $command = sprintf(
            'PGPASSWORD=%s psql -h %s -p %s -U %s -d %s -w -c %s',
            escapeshellarg($this->dbParams['#password']),
            escapeshellarg($this->dbParams['host']),
            escapeshellarg((string) $this->dbParams['port']),
            escapeshellarg($this->dbParams['user']),
            escapeshellarg($this->dbParams['database']),
            escapeshellarg($sql),
        );
        $process = Process::fromShellCommandline($command);
        $process->run();
        if ($process->getExitCode() !== 0) {
            throw new CopyAdapterException($process->getErrorOutput());
        }
    }

    protected function runCopyQuery(string $query, PgsqlExportConfig $exportConfig, string $csvPath): array
    {
        $copyCommand = sprintf(
            $exportConfig->hasQuery() ? // include header?
                "\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;" :
                "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;",
            rtrim($query, '; '),
            $csvPath
        );

        try {
            $this->runQuery($copyCommand);
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }

        return $this->analyseOutput($csvPath, $exportConfig);
    }

    private function analyseOutput(string $csvPath, ExportConfig $exportConfig): array
    {
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
        $this->logger->info(sprintf('Successfully exported %d rows.', $numRows));
        $output = ['rows' => $numRows];
        if ($exportConfig->isIncrementalFetching() && $lastFetchedRow) {
            $output['lastFetchedRow'] = $lastFetchedRow;
        }
        return $output;
    }

    private function getLastFetchedValue(
        PgsqlExportConfig $exportConfig,
        MetadataProvider $metadataProvider,
        array $lastExportedRow
    ): string {
        $columns = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() :
            $metadataProvider->getTable($exportConfig->getTable())->getColumns()->getNames();

        $columnIndex = array_search($exportConfig->getIncrementalFetchingColumn(), $columns);
        if ($columnIndex === false) {
            throw new CopyAdapterException(sprintf(
                'The specified incremental fetching column %s not found in the table',
                $exportConfig->getIncrementalFetchingColumn()
            ));
        }

        return $lastExportedRow[$columnIndex];
    }
}
