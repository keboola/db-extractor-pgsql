<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;

class PgSQLDbConnection extends PdoConnection
{
    public function quoteIdentifier(string $str): string
    {
        return "\"{$str}\"";
    }

    public function reconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $e) {
            $this->connect();
        }
    }
}
