<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\PgsqlApplication;
use Keboola\DbExtractor\Test\ExtractorTest;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractorLogger\Logger;
use Symfony\Component\Process\Process;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseTest extends ExtractorTest
{
    public const DRIVER = 'pgsql';

    protected Application $app;

    protected string $rootPath;

    /** @var string  */
    protected $dataDir = __DIR__ . '/../../data';

    protected array $dbConfig;

    protected function createDbProcess(string $query): Process
    {
        return Process::fromShellCommandline(
            sprintf(
                "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"$query\"",
                $this->dbConfig['#password'],
                $this->dbConfig['host'],
                $this->dbConfig['port'],
                $this->dbConfig['user'],
                $this->dbConfig['database']
            )
        );
    }

    protected function runProcesses(array $processes): void
    {
        foreach ($processes as $process) {
            $process->run();
            if (!$process->isSuccessful()) {
                $this->fail($process->getErrorOutput());
            }
        }
    }

    protected function cleanOutputDir(): void
    {
        $fs = new Filesystem();
        $fs->remove($this->dataDir . '/out/tables');
        $fs->mkdir($this->dataDir . '/out/tables');
    }

    public function setUp(): void
    {
        $this->cleanOutputDir();
        $this->rootPath = '/code/';
        $config = $this->getConfig();
        $logger = new Logger('ex-db-pgsql-tests');
        $this->app = new Application($config, $logger);

        $this->dbConfig = $config['parameters']['db'];

        // drop test tables
        $processes = [];
        // create a duplicate table in a different schema
        $processes[] = $this->createDbProcess(
            'CREATE SCHEMA IF NOT EXISTS testing;'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS escaping;'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS testing.escaping;'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS types_fk;'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS types;'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS auto_increment_timestamp'
        );
        $processes[] = $this->createDbProcess(
            'DROP TABLE IF EXISTS moving_targets'
        );
        $processes[] = $this->createDbProcess('DROP SEQUENCE IF EXISTS user_id_seq;');

        // create test tables
        $processes[] = $this->createDbProcess(
            'CREATE TABLE escaping (' .
            "\"_funnycol\" varchar(123) NOT NULL DEFAULT 'column 1', " .
            "\"_sadcol\" varchar(221) NOT NULL DEFAULT 'column 2', " .
            'PRIMARY KEY (\"_funnycol\", \"_sadcol\"));'
        );

        $processes[] = $this->createDbProcess(
            "\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV;"
        );

        $processes[] = $this->createDbProcess(
            'CREATE TABLE types ' .
            '(character varchar(123) PRIMARY KEY, ' .
            'integer integer NOT NULL DEFAULT 42, ' .
            'decimal decimal(5,3) NOT NULL DEFAULT 1.2, ' .
            'date date DEFAULT NULL);'
        );

        $processes[] = $this->createDbProcess(
            "\COPY types FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            'CREATE TABLE types_fk ' .
            '(character varchar(123) REFERENCES types (character), ' .
            'integer integer NOT NULL DEFAULT 42, ' .
            'decimal decimal(5,3) NOT NULL DEFAULT 1.2, ' .
            'date date DEFAULT NULL);'
        );

        $processes[] = $this->createDbProcess(
            "\COPY types_fk FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            'CREATE TABLE testing.escaping (' .
            "\"_funnycol\" varchar(123) NOT NULL DEFAULT 'column 1', " .
            "\"_sadcol\" varchar(221) NOT NULL DEFAULT 'column 2', " .
            'PRIMARY KEY ("_funnY$-col", "_sadcol"));'
        );
        $processes[] = $this->createDbProcess(
            "\COPY testing.escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' "
            . "WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            'CREATE TABLE IF NOT EXISTS empty_table ' .
            '(integer integer NOT NULL DEFAULT 42, ' .
            'date date DEFAULT NULL);'
        );

        $processes[] = $this->createDbProcess(
            "CREATE INDEX escaping_funnycol ON escaping(_funnycol);"
        );

        $this->runProcesses($processes);
    }

    public function getConfigRow(string $driver): array
    {
        $config = parent::getConfigRow($driver);
        $config['parameters']['extractor_class'] = 'PgSQL';
        return $config;
    }

    public function getConfig(string $driver = self::DRIVER): array
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'PgSQL';
        return $config;
    }

    protected function createApplication(array $config, array $state = [], ?Logger $logger = null): Application
    {
        return new PgsqlApplication(
            $config,
            $logger ? $logger : new Logger('ex-db-pgsql-tests'),
            $state,
            $this->dataDir
        );
    }

    public function configProvider(): array
    {
        $this->dataDir = __DIR__ . '/../../data';
        return [
            [$this->getConfig(self::DRIVER)],
            [$this->getConfigRow(self::DRIVER)],
        ];
    }
}
