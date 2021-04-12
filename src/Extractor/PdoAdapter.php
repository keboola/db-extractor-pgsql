<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Exception\InvalidStateException;

class PdoAdapter extends PdoExportAdapter
{
    protected function queryAndProcess(string $query, int $maxRetries, callable $processor): ExportResult
    {
        $connection = $this->connection;
        if (!$connection instanceof PgSQLDbConnection) {
            throw new InvalidStateException('PgSQLDbConnection expected.');
        }

        return $connection->queryAndProcess($query, $maxRetries, $processor, true);
    }
}
