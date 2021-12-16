<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

class CopyAdapterQueryMetadata implements QueryMetadata
{
    private PgSQLDbConnection $connection;

    private string $query;

    private ?ColumnCollection $columns = null;

    public function __construct(PgSQLDbConnection $connection, string $query)
    {
        $this->connection = $connection;
        $this->query = $query;
    }

    public function getColumns(): ColumnCollection
    {
        if ($this->columns === null) {
            $sql = sprintf('SELECT * FROM (%s) AS x LIMIT 0', rtrim($this->query, ';'));
            $stmt = $this->connection->getConnection()->prepare($sql);
            $stmt->execute();
            $this->columns = (new PdoQueryMetadata($stmt))->getColumns();
        }
        return $this->columns;
    }
}
