<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/02/16
 * Time: 17:49
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class PgSQL extends Extractor
{
    private $dbConfig;

    public function createConnection($dbParams)
    {
        $this->dbConfig = $dbParams;

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5432';

        $dsn = sprintf(
            "pgsql:host=%s;port=%s;dbname=%s;user=%s;password=%s",
            $dbParams['host'],
            $port,
            $dbParams['database'],
            $dbParams['user'],
            $dbParams['password']
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec("SET NAMES 'UTF8';");

        return $pdo;
    }

    /**
     * @param array $table
     * @return $outputTable output table name
     * @throws ApplicationException
     * @throws UserException
     * @throws \Keboola\Csv\Exception
     */
    public function export(array $table)
    {
        if (empty($table['outputTable'])) {
            throw new UserException("Missing attribute 'outputTable'");
        }
        $outputTable = $table['outputTable'];
        if (empty($table['query'])) {
            throw new UserException("Missing attribute 'query'");
        }
        $query = $table['query'];

        $this->logger->info("Exporting to " . $outputTable);

        $csv = $this->createOutputCsv($outputTable);

        $cursorName = 'exdbcursor' . intval(microtime(true));

        $curSql = "DECLARE $cursorName CURSOR FOR $query";

        try {
            $this->db->beginTransaction(); // cursors require a transaction.
            $stmt = $this->db->prepare($curSql);
            $stmt->execute();
            $innerStatement = $this->db->prepare("FETCH 1 FROM $cursorName");
            $innerStatement->execute();
        } catch (\PDOException $e) {
            throw new UserException("DB query failed: " . $e->getMessage(), 0, $e);
        }

        // write header and first line
        try {
            $resultRow = $innerStatement->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $e) {
            throw new UserException("DB query failed: " . $e->getMessage(), 0, $e);
        }

        if (is_array($resultRow) && !empty($resultRow)) {
            $csv->writeRow(array_keys($resultRow));

            if (isset($this->dbConfig['replaceNull'])) {
                $resultRow = $this->replaceNull($resultRow, $this->dbConfig['replaceNull']);
            }
            $csv->writeRow($resultRow);

            // write the rest
            try {
                $innerStatement = $this->db->prepare("FETCH 5000 FROM $cursorName");

                while ($innerStatement->execute() && count($resultRows = $innerStatement->fetchAll(\PDO::FETCH_ASSOC)) > 0) {
                    foreach ($resultRows as $resultRow) {
                        if (isset($this->dbConfig['replaceNull'])) {
                            $resultRow = $this->replaceNull($resultRow, $this->dbConfig['replaceNull']);
                        }
                        $csv->writeRow($resultRow);
                    }
                }

                // close the cursor
                $this->db->exec("CLOSE $cursorName");
                $this->db->commit();

            } catch (\PDOException $e) {
                throw new UserException("DB query failed: " . $e->getMessage(), 0, $e);
            }
        } else {
            $this->logger->warning("Query returned empty result. Nothing was imported.");
        }

        if ($this->createManifest($table) === false) {
            throw new ApplicationException("Unable to create manifest", 0, null, [
                'table' => $table
            ]);
        }

        return $outputTable;
    }

    private function replaceNull($row, $value)
    {
        foreach ($row as $k => $v) {
            if ($v === null) {
                $row[$k] = $value;
            }
        }

        return $row;
    }

}
