<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;
    use DropTableTrait;

    protected PDO $connection;

    protected function removeAllTables(): void
    {
        $sql = 'SELECT * FROM "information_schema"."tables" WHERE "table_schema" IN (\'public\', \'testing\')';

        $stmt = $this->connection->query($sql);
        if ($stmt) {
            $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
            foreach ((array) $tables as $table) {
                $this->dropTable($table['table_name']);
            }
        }

        $this->connection->query('DROP SEQUENCE IF EXISTS "user_id_seq"');
    }
}
