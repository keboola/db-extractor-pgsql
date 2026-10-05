<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Stubs;

use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Extractor\CursorQueryResult;
use Keboola\DbExtractor\Extractor\PgSQLDbConnection;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use PDOException;
use Psr\Log\NullLogger;
use Retry\BackOff\NoBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * Fails the first N queries the way a dropped SSL connection does, then answers from
 * canned metadata, so the reconnect-and-retry path can be asserted without a running Postgres.
 */
class FlakyPgSQLDbConnection extends PgSQLDbConnection
{
    public const SSL_EOF_ERROR = 'SQLSTATE[HY000]: General error: 7 SSL error: unexpected eof while reading';

    /** @var string[] */
    private array $queries = [];

    private int $reconnects = 0;

    public function __construct(private int $failuresBeforeSuccess, private ColumnCollection $columns)
    {
        // The parent constructor would open a real PDO connection
        $this->logger = new NullLogger();
    }

    /** @return string[] */
    public function getQueries(): array
    {
        return $this->queries;
    }

    public function getReconnectCount(): int
    {
        return $this->reconnects;
    }

    protected function connect(): void
    {
        $this->reconnects++;
    }

    protected function doQuery(
        string $query,
        bool $useCursor = false,
        int $batchSize = CursorQueryResult::DEFAULT_BATCH_SIZE,
    ): QueryResult {
        $this->queries[] = $query;
        if (count($this->queries) <= $this->failuresBeforeSuccess) {
            throw new PDOException(self::SSL_EOF_ERROR);
        }

        return new StubQueryResult([], new StubQueryMetadata($this->columns));
    }

    protected function createRetryProxy(int $maxRetries): RetryProxy
    {
        return new RetryProxy(
            new SimpleRetryPolicy($maxRetries, $this->getExpectedExceptionClasses()),
            new NoBackOffPolicy(),
            $this->logger,
        );
    }
}
