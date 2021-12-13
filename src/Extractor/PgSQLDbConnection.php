<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use PDOStatement;
use Throwable;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;

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
        bool $useCursor = false
    ): ExportResult {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query, $processor, $useCursor) {
                $dbResult = $this->queryReconnectOnError($query, $useCursor);
                // A db error can occur during fetching, so it must be wrapped/retried together
                $result = $processor($dbResult);
                // Success of isAlive means that ALL data has been extracted
                $this->isAlive();
                return $result;
            }
        );
    }

    public function createQueryMetadata(string $query): QueryMetadata
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();
        return new PdoQueryMetadata($stmt);
    }

    protected function queryReconnectOnError(string $query, bool $useCursor = false): QueryResult
    {
        try {
            return $this->doQuery($query, $useCursor);
        } catch (Throwable $e) {
            try {
                // Reconnect
                $this->connect();
            } catch (Throwable $e) {
            };
            throw $e;
        }
    }

    protected function doQuery(string $query, bool $useCursor = false): QueryResult
    {
        return $useCursor ? $this->doQueryWithCursor($query) : parent::doQuery($query);
    }

    protected function doQueryWithCursor(string $query): CursorQueryResult
    {
        return new CursorQueryResult(
            $this->pdo,
            $this->logger,
            $query,
            $this->createQueryMetadata($query)
        );
    }
}
