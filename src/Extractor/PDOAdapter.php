<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Exception\ApplicationException;
use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryResult;
use Keboola\DbExtractor\Adapter\ResultWriter\ResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDOException;
use Throwable;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;

class PDOAdapter extends PdoExportAdapter
{

    /** @var PgSQLDbConnection $connection */
    protected DbConnection $connection;

    /** @var PgSQLResultWriter $resultWriter */
    protected ResultWriter $resultWriter;

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!($this->resultWriter instanceof PgSQLResultWriter)) {
            throw new InvalidArgumentException('Class ResultWriter must be an instance PgsqlResultWriter class.');
        }
        if (!$exportConfig instanceof PgsqlExportConfig) {
            throw new InvalidArgumentException('PgsqlExportConfig expected.');
        }

        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);

        try {
            return $this->queryAndProcess(
                $query,
                $exportConfig,
                function (
                    QueryResult $result,
                    bool $resetSettings,
                    ?callable $rowCallback
                ) use (
                    $exportConfig,
                    $csvFilePath
                ) {
                    return $this->resultWriter->writeToCsv(
                        $result,
                        $exportConfig,
                        $csvFilePath,
                        $resetSettings,
                        $rowCallback
                    );
                }
            );
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (PDOException $pdoError) {
            throw new UserException(sprintf('Error executing: %s', $pdoError->getMessage()));
        } catch (UserExceptionInterface $e) {
            throw $this->handleDbError($e, $exportConfig->getMaxRetries(), $exportConfig->getOutputTable());
        }
    }

    public function queryAndProcess(string $query, PgsqlExportConfig $exportConfig, callable $callable): ExportResult
    {
        $retryProxy = new DbRetryProxy($this->logger, $exportConfig->getMaxRetries());
        return $retryProxy
            ->call(function () use ($query, $exportConfig, $callable) {
                try {
                    $result = $this->executeQueryPDO($query, $exportConfig, $callable);
                    $this->connection->isAlive();
                    return $result;
                } catch (Throwable $queryError) {
                    try {
                        $this->connection->reconnect();
                    } catch (Throwable $connectionError) {
                    }
                    throw $queryError;
                }
            });
    }

    protected function executeQueryPDO(string $query, PgsqlExportConfig $exportConfig, callable $callable): ExportResult
    {
        $rowCallback = null;
        if ($exportConfig->getReplaceBooleans()) {
            $rowCallback = function (array $row) {
                array_walk($row, function (&$item): void {
                    if (is_bool($item)) {
                        $item = $item === true ? 't' : 'f';
                    }
                });
                return $row;
            };
        }
        $cursorName = 'exdbcursor' . intval(microtime(true));
        $curSql = "DECLARE $cursorName CURSOR FOR $query";
        try {
            $this->connection->getConnection()->beginTransaction(); // cursors require a transaction.
            $stmt = $this->connection->getConnection()->prepare($curSql);
            $stmt->execute();

            // write the rest
            $this->logger->info('Fetching data...');
            $innerStatement = $this->connection->getConnection()->prepare("FETCH 10000 FROM $cursorName");

            $firstLoop = true;
            while ($innerStatement->execute()) {
                $queryResult = new PdoQueryResult($innerStatement);

                /** @var ExportResult $exportResult */
                $exportResult = $callable(
                    $queryResult,
                    $firstLoop,
                    $rowCallback
                );

                if (!$queryResult->getIterator()->valid()) {
                    break;
                }

                $firstLoop = false;
            }
            // close the cursor
            $this->connection->getConnection()->exec("CLOSE $cursorName");
            $this->connection->getConnection()->commit();

            $this->logger->info(
                sprintf('Extraction completed. Fetched %s rows.', $exportResult->getRowsCount())
            );

            return $exportResult;
        } catch (PDOException $e) {
            try {
                $this->connection->getConnection()->rollBack();
            } catch (Throwable $e2) {
            }
            $innerStatement = null;
            $stmt = null;
            $message = preg_replace('/exdbcursor([0-9]+)/', 'exdbcursor', $e->getMessage());
            throw new PDOException((string) $message, 0, $e->getPrevious());
        }
    }
}
