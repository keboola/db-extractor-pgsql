<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class ApplicationTest extends ExtractorTest
{
    /** @var array */
    private $dbConfig;

    /** @var  string */
    protected $rootPath;

    /** @var string  */
    protected $dataDir = __DIR__ . '/../../data';

    public function setUp(): void
    {
        $this->rootPath = __DIR__ . '/../../..';

        parent::setUp();
        if (getenv('EXTERNAL_PG_HOST') === false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_HOST'");
        }
        if (getenv('EXTERNAL_PG_DATABASE') === false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_DATABASE'");
        }
        if (getenv('EXTERNAL_PG_USER') === false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_USER'");
        }
        if (getenv('EXTERNAL_PG_PASSWORD') === false) {
            $this->fail("Missing environment var 'EXTERNAL_PG_PASSWORD'");
        }
        $this->dbConfig['host'] = getenv('EXTERNAL_PG_HOST');
        $this->dbConfig['database'] = getenv('EXTERNAL_PG_DATABASE');
        $this->dbConfig['user'] = getenv('EXTERNAL_PG_USER');
        $this->dbConfig['password'] = getenv('EXTERNAL_PG_PASSWORD');
        $this->dbConfig['port'] = (!is_null(getenv('EXTERNAL_PG_PORT'))) ? getenv('EXTERNAL_PG_PORT') : 5432;
    }

    public function testRunAction(): void
    {
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/in.c-main.info_schema.csv');
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.info_schema.csv.manifest';
        @unlink($outputCsvFile->getPathname());
        @unlink($manifestFile);

        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
    }

    public function testTestConnectionAction(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'testConnection';
        $config['parameters']['db'] = $this->dbConfig;
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTrailingSemicolonQuery(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        $config['parameters']['tables'][0]['query'] = $config['parameters']['tables'][0]['query'] . ";";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testProcessTimeout(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        $config['parameters']['tables'][0]['query'] = "SELECT pg_sleep(65), 1";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testUserError(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        $config['parameters']['tables'][0]['query'] = "SELECT something, fake";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        
        $this->assertFalse(strstr($process->getErrorOutput(), "PGPASSWORD"));
        $this->assertContains($config['parameters']['tables'][0]['name'], $process->getErrorOutput());
        $this->assertEquals(1, $process->getExitCode());
    }

    public function testPDOFallback(): void
    {
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/in.c-main.info_schema.csv');
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.info_schema.csv.manifest';
        @unlink($outputCsvFile->getPathname());
        @unlink($manifestFile);

        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        // queries with comments will break the () in the \copy command.
        // Failed \copy commands should fallback to using the old PDO method
        $config['parameters']['tables'][0]['query'] = "
            select * from information_schema.TABLES as tables JOIN (
                -- this is a comment --
                select * from information_schema.columns
            ) as columns ON tables.table_schema = columns.table_schema AND tables.table_name = columns.table_name;
        ";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        // valid query should not error
        $this->assertContains('Failed \copy command', $process->getOutput());
        // assert that PDO attempt succeeded
        $this->assertEquals(0, $process->getExitCode());
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
    }

    public function testGetTablesAction(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'getTables';
        $config['parameters']['db'] = $this->dbConfig;
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertJson($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testCommentQuery(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        @unlink($this->dataDir . '/config.yml');
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM information_schema.tables -- with some comment";
        $config['parameters']['db'] = $this->dbConfig;
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertNotContains("Failed \copy command", $process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
    }
}
