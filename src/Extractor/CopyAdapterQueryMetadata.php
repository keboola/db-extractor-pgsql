<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use PDOException;

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
            // The PDO connection idles during the psql \copy export and may be dropped; query() reconnects and retries
            $result = $this->connection->query($sql, DbConnection::DEFAULT_MAX_RETRIES);
            try {
                $this->columns = $result->getMetadata()->getColumns();
            } catch (PDOException $e) {
                throw new UserException($e->getMessage(), 0, $e);
            }
        }
        return $this->columns;
    }
}
