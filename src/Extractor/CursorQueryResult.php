<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Throwable;
use PDO;
use PDOStatement;
use PDOException;
use Iterator;
use Keboola\DbExtractor\Exception\NotImplementedException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Psr\Log\LoggerInterface;

class CursorQueryResult implements QueryResult
{
    public const BATCH_SIZE = 10000;

    public const LOG_PROGRESS_SECONDS = 60;

    private PDO $pdo;

    private LoggerInterface $logger;

    private string $query;

    private string $cursorName;

    private PDOStatement $batchStmt;

    public function __construct(PDO $pdo, LoggerInterface $logger, string $query)
    {
        $this->pdo = $pdo;
        $this->logger = $logger;
        $this->query = $query;
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
                self::BATCH_SIZE,
                $this->cursorName
            ));
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
        $this->logger->info(sprintf('Fetching rows, batch size = %d ...', self::BATCH_SIZE));

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
}
