<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/02/16
 * Time: 17:49
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

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
            "pgsql:host=%s;port=%s;dbname=%s",
            $dbParams['host'],
            $port,
            $dbParams['database']
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec("SET NAMES 'UTF8';");

        return $pdo;
    }

    private function restartConnection()
    {
        $this->db = null;
        try {
            $this->db = $this->createConnection($this->dbConfig);
        } catch (\Exception $e) {
            throw new UserException(sprintf("Error connecting to DB: %s", $e->getMessage()), 0, $e);
        }
    }

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];

        $this->logger->info("Exporting to " . $outputTable);

        $query = $table['query'];

        $tries = 0;
        $exception = null;

        $csvCreated = false;
        while ($tries < 5) {
            $exception = null;
            try {
                if ($tries > 0) {
                    $this->logger->info("Retrying query");
                    $this->restartConnection();
                }
                $csvCreated = $this->executeQuery($query, $this->createOutputCsv($outputTable));
                break;
            } catch (\PDOException $e) {
                $exception = new UserException("DB query failed: " . $e->getMessage(), 0, $e);
            }

            sleep(pow($tries, 2));
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }

        if ($csvCreated) {
            if ($this->createManifest($table) === false) {
                throw new ApplicationException("Unable to create manifest", 0, null, [
                    'table' => $table
                ]);
            }
        }

        return $outputTable;
    }

    protected function executeQuery($query, CsvFile $csvFile)
    {
        $this->logger->info("Executing query...");

        $command = sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;\"",
            $this->dbConfig['password'],
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['user'],
            $this->dbConfig['database'],
            rtrim($query, ';'),
            $csvFile
        );

        $process = new Process($command);
        // allow it to run for as long as it needs
        $process->setTimeout(null);
        $process->run();
        if (!$process->isSuccessful()) {
            $cleanError = preg_replace('/PGPASSWORD\=\'.*\'/', 'REDACTED', $process->getErrorOutput());
            $this->logger->error($cleanError);
            throw new UserException("Error occurred copying query output to file.");
        }
        return true;
    }

    public function testConnection()
    {
        // check PDO connection
        $this->db->query("SELECT 1");

        // check psql connection
        $command = sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"SELECT 1;\"",
            $this->dbConfig['password'],
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['user'],
            $this->dbConfig['database']
        );
        $process = new Process($command);
        $process->run();
        if ($process->getExitCode() !== 0) {
            throw new UserException("Failed psql connection: " . $process->getErrorOutput());
        }
    }
}
