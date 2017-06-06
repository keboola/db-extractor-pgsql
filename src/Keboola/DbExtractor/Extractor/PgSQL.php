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

    private function writePgPass()
    {
        $passfile = new \SplFileObject("/root/.pgpass", 'w');

        $passfile->fwrite(sprintf(
            "%s:%s:%s:%s:%s",
            $this->dbConfig['host'],
            ($this->dbConfig['port']) ? $this->dbConfig['port'] : "5432",
            $this->dbConfig['database'],
            $this->dbConfig['user'],
            $this->dbConfig['password']
        ));
    }

    public function createConnection($dbParams)
    {
        $this->dbConfig = $dbParams;

        $this->writePgPass();
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
            "psql -h pgsql -U %s -d %s -w -c \"\COPY (%s) TO '%s' WITH CSV HEADER DELIMITER ',' FORCE QUOTE *;\"",
            $this->dbConfig['user'],
            $this->dbConfig['database'],
            $query,
            $csvFile
        );
        $process = new Process($command);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->logger->error($process->getErrorOutput());
            throw new \Exception("Error occurred copying query output to file.");
        }
        return true;
    }

    public function testConnection()
    {
        $this->db->query("SELECT 1");
    }
}
