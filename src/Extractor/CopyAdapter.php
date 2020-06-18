<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\CopyAdapterConnectionException;
use Keboola\DbExtractor\Exception\CopyAdapterException;
use Keboola\DbExtractor\Exception\CopyAdapterQueryException;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Process\Process;

class CopyAdapter
{
    private Logger $logger;

    private array $dbParams;

    private array $state;

    public function __construct(Logger $logger, array $dbParams, array $state)
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
        bool $advancedQuery,
        array $columnMetadata,
        ?array $incrementalFetching,
        CsvFile $csvFile
    ): array {
        $result = $this->runCopyQuery($query, $advancedQuery, $incrementalFetching, $csvFile);
        if (!$advancedQuery && isset($result['lastFetchedRow'])) {
            $result['lastFetchedRow'] = $this->getLastFetchedValue(
                $columnMetadata,
                $incrementalFetching,
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

    protected function runCopyQuery(
        string $query,
        bool $includeHeader,
        ?array $incrementalFetching,
        CsvFile $csvFile
    ): array {
        $copyCommand = sprintf(
            $includeHeader ?
                "\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;" :
                "\COPY (%s) TO '%s' WITH CSV DELIMITER ',' FORCE QUOTE *;",
            rtrim($query, '; '),
            $csvFile
        );

        try {
            $this->runQuery($copyCommand);
        } catch (CopyAdapterException $e) {
            throw new CopyAdapterQueryException($e->getMessage(), 0, $e);
        }

        return $this->analyseOutput($csvFile, $incrementalFetching);
    }

    private function analyseOutput(CsvFile $csvFile, ?array $incrementalFetching): array
    {
        // Get the number of written rows and lastFetchedValue
        $outputFile = $csvFile;
        $numRows = 0;
        $lastFetchedRow = null;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                throw new CopyAdapterException('The \copy command produced an invalid csv.');
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
        if ($lastFetchedRow && isset($incrementalFetching['column'])) {
            $output['lastFetchedRow'] = $lastFetchedRow;
        }
        return $output;
    }

    private function getLastFetchedValue(
        array $columnMetadata,
        ?array $incrementalFetching,
        array $lastExportedRow
    ): string {
        foreach ($columnMetadata as $key => $column) {
            if ($column['name'] === $incrementalFetching['column']) {
                return $lastExportedRow[$key];
            }
        }

        throw new CopyAdapterException(
            sprintf(
                'The specified incremental fetching column %s not found in the table',
                $incrementalFetching['column']
            )
        );
    }
}
