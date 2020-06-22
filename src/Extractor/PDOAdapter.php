<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;
use PDOException;
use Retry\RetryProxy;
use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;

class PDOAdapter
{
    private LoggerInterface $logger;

    private PDO $pdo;

    private DatabaseConfig $databaseConfig;

    private array $state;

    public function __construct(LoggerInterface $logger, DatabaseConfig $databaseConfig, array $state)
    {
        $this->logger = $logger;
        $this->databaseConfig = $databaseConfig;
        $this->state = $state;

        if (!$databaseConfig->hasDatabase()) {
            throw new UserException(sprintf('Parameter "database" is missing.', $r));
        }

        $this->createConnection();
    }

    public function testConnection(): void
    {
        $this->pdo->query('SELECT 1');
    }

    public function export(string $query, PgsqlExportConfig $exportConfig, string $csvPath): array
    {
        // Check connection
        $this->tryReconnect();

        return $this
            ->createRetryProxy($exportConfig->getMaxRetries())
            ->call(function () use ($query, $exportConfig, $csvPath) {
                try {
                    // Csv writer must be re-created after each error, because some lines could be already written
                    $csv = new CsvWriter($csvPath);
                    $result =  $this->executeQueryPDO($query, $exportConfig, $csv);
                    $this->isAlive();
                    return $result;
                } catch (Throwable $queryError) {
                    try {
                        $this->createConnection();
                    } catch (Throwable $connectionError) {
                    }
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

    protected function executeQueryPDO(string $query, PgsqlExportConfig $exportConfig, CsvWriter $csv): array
    {
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
                if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
                    $output['lastFetchedRow'] = $this->state['lastFetchedRow'];
                }
                $output['rows'] = 0;
                return $output;
            }
            if ($exportConfig->getReplaceBooleans()) {
                $resultRow = $this->replaceBooleanValues($resultRow);
            }
            // only write header for advanced query case
            if ($exportConfig->hasQuery()) {
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
                    if ($exportConfig->getReplaceBooleans()) {
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
            if ($exportConfig->isIncrementalFetching()) {
                if (!array_key_exists($exportConfig->getIncrementalFetchingColumn(), $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $exportConfig->getIncrementalFetchingColumn()
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$exportConfig->getIncrementalFetchingColumn()];
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

        $port = $this->databaseConfig->hasPort() ? $this->databaseConfig->getPort() : '5432';

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            $this->databaseConfig->getHost(),
            $port,
            $this->databaseConfig->getDatabase()
        );
        var_dump($dsn);
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        $this->pdo = new PDO(
            $dsn,
            $this->databaseConfig->getUsername(),
            $this->databaseConfig->getPassword(),
            $options
        );
        $this->pdo->exec("SET NAMES 'UTF8';");
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $e) {
            $reconnectionRetryProxy = new DbRetryProxy(
                $this->logger,
                DbRetryProxy::DEFAULT_MAX_TRIES,
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
