<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Throwable;

class PgSQLDbConnection extends PdoConnection
{
    public function quoteIdentifier(string $str): string
    {
        return "\"{$str}\"";
    }

    public function queryAndProcess(
        string $query,
        int $maxRetries,
        callable $processor,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
        bool $useCursor = false,
    ): ExportResult {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query, $processor, $useCursor, $batchSize) {
                $dbResult = $this->queryReconnectOnError($query, $useCursor, $batchSize);
                // A db error can occur during fetching, so it must be wrapped/retried together
                $result = $processor($dbResult);
                // Success of isAlive means that ALL data has been extracted
                $this->isAlive();
                return $result;
            },
        );
    }

    protected function queryReconnectOnError(
        string $query,
        bool $useCursor = false,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
    ): QueryResult {
        $this->logger->debug(sprintf('Running query "%s".', $query));
        try {
            return $this->doQuery($query, $useCursor, $batchSize);
        } catch (Throwable $e) {
            try {
                // Reconnect
                $this->connect();
            } catch (Throwable $e) {
            };
            throw $e;
        }
    }

    protected function doQuery(
        string $query,
        bool $useCursor = false,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
    ): QueryResult {
        return $useCursor ? $this->doQueryWithCursor($query, $batchSize) : parent::doQuery($query);
    }

    protected function doQueryWithCursor(
        string $query,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
    ): CursorQueryResult {
        return new CursorQueryResult(
            $this->pdo,
            $this->logger,
            $query,
            $batchSize,
        );
    }
}
