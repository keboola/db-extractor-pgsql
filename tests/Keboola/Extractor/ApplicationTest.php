<?php

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class ApplicationTest extends ExtractorTest
{
    /**
     * @var array
     */
    private $dbConfig;

    public function setUp()
    {
        parent::setUp();
        if (getenv('EXTERNAL_PG_HOST') !== false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_HOST'");
        }
        if (getenv('EXTERNAL_PG_DATABASE') !== false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_DATABASE'");
        }
        if (getenv('EXTERNAL_PG_USER') !== false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_USER'");
        }
        if (getenv('EXTERNAL_PG_PASSWORD') !== false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_PASSWORD'");
        }
        $this->dbConfig['host'] = getenv('EXTERNAL_PG_HOST');
        $this->dbConfig['database'] = getenv('EXTERNAL_PG_DATABASE');
        $this->dbConfig['user'] = getenv('EXTERNAL_PG_USER');
        $this->dbConfig['password'] = getenv('EXTERNAL_PG_PASSWORD');
        $this->dbConfig['port'] = (!is_null(getenv('EXTERNAL_PG_PORT'))) ? getenv('EXTERNAL_PG_PORT') : 5432;
    }

    public function testRunAction()
    {
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/in.c-main.info_schema.csv');
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.info_schema.csv.manifest';
        @unlink($outputCsvFile);
        @unlink($manifestFile);

        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        var_dump($config);
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        var_dump($process->getOutput());
        var_dump($process->getErrorOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
    }

    public function testTestConnectionAction()
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'testConnection';
        $config['parameters']['db'] = $this->dbConfig;
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        var_dump($process->getOutput());
        var_dump($process->getErrorOutput());
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

    }
}