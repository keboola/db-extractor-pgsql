<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;

class PgsqlTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-pgsql');
        }
        $config = $this->getConfig();
        $this->app = new Application($config);

        $dbConfig = $config['parameters']['db'];

        // Create the pass file to be able to use psql when password required
        $passfile = new \SplFileObject("/root/.pgpass", 'w');
        $passfile->fwrite(sprintf(
            "%s:%s:%s:%s:%s",
            $dbConfig['host'],
            ($dbConfig['port']) ? $dbConfig['port'] : "5432",
            $dbConfig['database'],
            $dbConfig['user'],
            $dbConfig['password']
        ));

        // create test tables
        $process = new Process(sprintf(
            "psql -h %s -p %s -U %s -d %s -w -c \"DROP TABLE IF EXISTS escaping;\"",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "psql -h %s -p %s -U %s -d %s -w -c \"CREATE TABLE escaping (col1 VARCHAR NOT NULL, col2 VARCHAR NOT NULL);\"",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "psql -h %s -p %s -U %s -d %s -w -c \"\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV HEADER;\"",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
    }

    public function getConfig($driver = 'pgsql')
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'PgSQL';
        return $config;
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = new CsvFile(ROOT_PATH . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals($expectedCsvFile->getHeader(), $outputCsvFile->getHeader());
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertEquals($expectedArr[$i], $outputArr[$i]);
        }
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('pgsql', 'DB_SSH_KEY_PRIVATE', true),
                'public' => $this->getEnv('pgsql', 'DB_SSH_KEY_PUBLIC', true)
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy'
        ];

        $app = new Application($config);
        $result = $app->run();

        $expectedCsvFile = new CsvFile(ROOT_PATH . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals($expectedCsvFile->getHeader(), $outputCsvFile->getHeader());
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertEquals($expectedArr[$i], $outputArr[$i]);
        }
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = new Application($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }
}
