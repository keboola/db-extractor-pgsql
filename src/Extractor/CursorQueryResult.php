<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Iterator;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;

class CursorQueryResult implements QueryResult
{
    public const DEFAULT_BATCH_SIZE = 10000;

    public const LOG_PROGRESS_SECONDS = 60;

    private PDO $pdo;

    private LoggerInterface $logger;

    private int $batchSize;

    private string $query;

    private string $cursorName;

    private PDOStatement $batchStmt;

    protected QueryMetadata $queryMetadata;

    public function __construct(
        PDO $pdo,
        LoggerInterface $logger,
        string $query,
        int $batchSize = self::DEFAULT_BATCH_SIZE,
    ) {
        $this->pdo = $pdo;
        $this->logger = $logger;
        $this->query = $query;
        $this->batchSize = $batchSize;

        $this->cursorName = 'exdbcursor' . intval(microtime(true));

        // Start transaction and create cursor
        try {
            $this->pdo->beginTransaction(); // cursors require a transaction
            $cursorSql = sprintf('DECLARE %s CURSOR FOR %s', $this->cursorName, $this->query);
            $cursorStmt = $this->pdo->prepare($cursorSql);
            $cursorStmt->execute();

            // Fetch statement is re-used
            $this->batchStmt = $this->pdo->prepare(sprintf(
                'FETCH %d FROM %s',
                $this->batchSize,
                $this->cursorName,
            ));
            $this->queryMetadata = new PdoQueryMetadata($this->batchStmt);
        } catch (PDOException $e) {
            $this->closeCursor();
            $message = preg_replace('/exdbcursor([0-9]+)/', 'exdbcursor', $e->getMessage());
            throw new PDOException((string) $message, 0, $e->getPrevious());
        }
    }

    public function getIterator(): Iterator
    {
        try {
            return $this->doGetIterator();
        } catch (PDOException $e) {
            $this->closeCursor();
            $message = preg_replace('/exdbcursor([0-9]+)/', 'exdbcursor', $e->getMessage());
            throw new PDOException((string) $message, 0, $e->getPrevious());
        }
    }

    public function fetch(): ?array
    {
        $iterator = $this->getIterator();
        if (!$iterator->valid()) {
            return null;
        }

        // Get value and move forward
        $value = $iterator->current();
        $iterator->next();
        return $value;
    }

    public function fetchAll(): array
    {
        return iterator_to_array($this->getIterator());
    }

    public function closeCursor(): void
    {
        try {
            $this->pdo->exec("CLOSE $this->cursorName");
        } catch (Throwable $e) {
            // ignore
        }

        $this->closeTransaction();
    }

    public function getResource(): PDOStatement
    {
        return $this->batchStmt;
    }

    protected function closeTransaction(): void
    {
        try {
            $this->pdo->rollBack();
        } catch (Throwable $e) {
            // ignore
        }
    }

    protected function doGetIterator(): Iterator
    {
        $this->logger->info(sprintf('Fetching rows, batch size = %d ...', $this->batchSize));

        // Process next batch
        $allRows = 0;
        $lastProgressLog = microtime(true);
        while ($this->batchStmt->execute()) {
            // Yield all rows from batch
            $batchRows = 0;
            while ($row = $this->batchStmt->fetch(PDO::FETCH_ASSOC)) {
                $batchRows++;
                $allRows++;
                yield $row;
            }

            // All result fetched?
            if ($batchRows === 0) {
                break;
            }

            // Log progress each N seconds
            $now = microtime(true);
            if ($lastProgressLog + self::LOG_PROGRESS_SECONDS <= $now) {
                $lastProgressLog = $now;
                $this->logger->info(sprintf('Fetched "%d" rows so far.', $allRows));
            }
        }
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getMetadata(): QueryMetadata
    {
        return $this->queryMetadata;
    }
}
