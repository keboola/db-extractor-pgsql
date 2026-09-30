<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Stubs;

use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Extractor\PgSQLDbConnection;
use LogicException;

/**
 * Records the SQL it is given and replays canned rows, so the metadata query
 * can be asserted without a running Postgres.
 */
class CapturingPgSQLDbConnection extends PgSQLDbConnection
{
    private const SERVER_VERSION_MARKER = 'server_version_num';

    /** @var string[] */
    private array $queries = [];

    /**
     * @param array<array<string, mixed>> $rows rows returned for the metadata query
     */
    public function __construct(private array $rows = [])
    {
    }

    public function query(string $query, int $maxRetries = 1): QueryResult
    {
        $this->queries[] = $query;

        // The provider asks for the server version before the metadata query
        if (str_contains($query, self::SERVER_VERSION_MARKER)) {
            return new StubQueryResult([[self::SERVER_VERSION_MARKER => '160000']]);
        }

        return new StubQueryResult($this->rows);
    }

    public function quote(string $str): string
    {
        return "'" . $str . "'";
    }

    public function getMetadataQuery(): string
    {
        foreach ($this->queries as $query) {
            if (!str_contains($query, self::SERVER_VERSION_MARKER)) {
                return $query;
            }
        }

        throw new LogicException('No metadata query was executed.');
    }
}
