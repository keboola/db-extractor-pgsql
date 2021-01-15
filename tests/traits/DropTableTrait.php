<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use \PDO;

trait DropTableTrait
{
    use QuoteIdentifierTrait;
    protected PDO $connection;

    public function dropTable(string $tableName): void
    {
        $sql = sprintf(
            'DROP TABLE IF EXISTS %s CASCADE',
            $this->quoteIdentifier($tableName)
        );
        $this->connection->query($sql);
    }
}
