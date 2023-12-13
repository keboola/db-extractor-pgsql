<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractor\Adapter\Exception\ApplicationException;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\Exception\InvalidStateException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;

class PdoAdapter extends PdoExportAdapter
{
    /**
     * @param PgsqlExportConfig $exportConfig
     */
    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);
        try {
            return $this->queryAndProcess(
                $query,
                $exportConfig->getMaxRetries(),
                function (QueryResult $result) use ($exportConfig, $csvFilePath) {
                    return $this->resultWriter->writeToCsv($result, $exportConfig, $csvFilePath);
                },
                $exportConfig->getBatchSize(),
            );
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (UserExceptionInterface $e) {
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $exportConfig->getOutputTable());
        }
    }

    protected function queryAndProcess(
        string $query,
        int $maxRetries,
        callable $processor,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
    ): ExportResult {
        $connection = $this->connection;
        if (!$connection instanceof PgSQLDbConnection) {
            throw new InvalidStateException('PgSQLDbConnection expected.');
        }

        return $connection->queryAndProcess($query, $maxRetries, $processor, $batchSize, true);
    }
}
