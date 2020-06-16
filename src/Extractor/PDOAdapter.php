<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Psr\Log\LoggerInterface;
use Throwable;
use PDO;
use PDOException;
use Retry\RetryProxy;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;

class PDOAdapter
{
    private LoggerInterface $logger;

    private PDO $pdo;

    private array $dbParams;

    private array $state;

    public function __construct(LoggerInterface $logger, array $dbParams, array $state)
    {
        $this->logger = $logger;
        $this->dbParams = $dbParams;
        $this->state = $state;

        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $this->createConnection();
    }

    public function testConnection(): void
    {
        $this->pdo->query('SELECT 1');
    }

    public function export(
        string $query,
        bool $advancedQuery,
        int $maxTries,
        bool $replaceBooleans,
        ?array $incrementalFetching,
        CsvFile $csvFile
    ): array {
        // Check connection
        $this->tryReconnect();

        return $this
            ->createRetryProxy($maxTries)
            ->call(function () use (
                $query,
                $csvFile,
                $advancedQuery,
                $replaceBooleans,
                $incrementalFetching
            ) {
                try {
                    return $this->executeQueryPDO(
                        $query,
                        $csvFile,
                        $advancedQuery,
                        $replaceBooleans,
                        $incrementalFetching,
                    );
                } catch (Throwable $queryError) {
                    try {
                        $this->createConnection();
                    } catch (Throwable $connectionError) {
                    };
                    throw $queryError;
                }
            });
    }

    public function runRetryableQuery(string $query, array $values = []): array
    {
        $retryProxy = new DbRetryProxy($this->logger);
        return $retryProxy->call(function () use ($query, $values) {
            try {
                $stmt = $this->pdo->prepare($query);
                $stmt->execute($values);
                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            } catch (Throwable $exception) {
                $this->tryReconnect();
                throw $exception;
            }
        });
    }

    public function quote(string $str): string
    {
        return $this->pdo->quote($str);
    }

    public function quoteIdentifier(string $str): string
    {
        return "\"{$str}\"";
    }

    protected function executeQueryPDO(
        string $query,
        CsvFile $csv,
        bool $advancedQuery,
        bool $replaceBooleans,
        ?array $incrementalFetching
    ): array {
        $cursorName = 'exdbcursor' . intval(microtime(true));
        $curSql = "DECLARE $cursorName CURSOR FOR $query";
        try {
            $this->pdo->beginTransaction(); // cursors require a transaction.
            $stmt = $this->pdo->prepare($curSql);
            $stmt->execute();
            $innerStatement = $this->pdo->prepare("FETCH 1 FROM $cursorName");
            $innerStatement->execute();
            // write header and first line
            $resultRow = $innerStatement->fetch(PDO::FETCH_ASSOC);
            if (!is_array($resultRow) || empty($resultRow)) {
                // no rows found.  If incremental fetching is turned on, we need to preserve the last state
                if (isset($incrementalFetching['column']) && isset($this->state['lastFetchedRow'])) {
                    $output['lastFetchedRow'] = $this->state['lastFetchedRow'];
                }
                $output['rows'] = 0;
                return $output;
            }
            if ($replaceBooleans) {
                $resultRow = $this->replaceBooleanValues($resultRow);
            }
            // only write header for advanced query case
            if ($advancedQuery) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            $numRows = 1;
            $lastRow = $resultRow;
            // write the rest
            $this->logger->info('Fetching data...');
            $innerStatement = $this->pdo->prepare("FETCH 10000 FROM $cursorName");

            while ($innerStatement->execute()) {
                /** @var array $resultRows */
                $resultRows = $innerStatement->fetchAll(PDO::FETCH_ASSOC);
                if (count($resultRows) === 0) {
                    break;
                }

                foreach ($resultRows as $resultRow) {
                    if ($replaceBooleans) {
                        $resultRow = $this->replaceBooleanValues($resultRow);
                    }
                    $csv->writeRow($resultRow);
                    $lastRow = $resultRow;
                    $numRows++;
                }
            }
            // close the cursor
            $this->pdo->exec("CLOSE $cursorName");
            $this->pdo->commit();

            // get last fetched value
            if (isset($incrementalFetching['column'])) {
                if (!array_key_exists($incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            $this->logger->info("Extraction completed. Fetched {$numRows} rows.");
        } catch (PDOException $e) {
            try {
                $this->pdo->rollBack();
            } catch (Throwable $e2) {
            }
            $innerStatement = null;
            $stmt = null;
            throw $e;
        }
        return $output;
    }

    private function replaceBooleanValues(array $row): array
    {
        array_walk($row, function (&$item): void {
            if (is_bool($item)) {
                $item = $item === true ? 't' : 'f';
            }
        });
        return $row;
    }

    private function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    private function createConnection(): void
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 60,
        ];

        $port = isset($this->dbParams['port']) ? $this->dbParams['port'] : '5432';

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            $this->dbParams['host'],
            $port,
            $this->dbParams['database']
        );
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        $this->pdo = new PDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
        $this->pdo->exec("SET NAMES 'UTF8';");
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $e) {
            $reconnectionRetryProxy = new DbRetryProxy(
                $this->logger,
                Extractor::DEFAULT_MAX_TRIES,
                null,
                1000
            );
            try {
                $reconnectionRetryProxy->call(function (): void {
                    $this->createConnection();
                });
            } catch (Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    $reconnectException->getCode(),
                    $reconnectException
                );
            }
        }
    }

    private function createRetryProxy(int $maxTries): RetryProxy
    {
        return new DbRetryProxy($this->logger, $maxTries);
    }
}
