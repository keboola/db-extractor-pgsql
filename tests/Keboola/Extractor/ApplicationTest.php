<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Symfony\Component\Filesystem;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;
use Keboola\DbExtractor\Exception\UserException;

class ApplicationTest extends BaseTest
{
    private function replaceConfig(array $config, string $format): void
    {
        @unlink($this->dataDir . '/config.json');
        @unlink($this->dataDir . '/config.yml');
        if ($format === self::CONFIG_FORMAT_JSON) {
            file_put_contents($this->dataDir . '/config.json', json_encode($config));
        } else if ($format === self::CONFIG_FORMAT_YAML) {
            file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));
        } else {
            throw new UserException("Invalid config format type [{$format}]");
        }
    }

    /**
     * @dataProvider configProvider
     */
    public function testRunAction(array $config, string $format): void
    {
        $this->replaceConfig($config, $format);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTestConnectionAction(): void
    {
        $config['action'] = 'testConnection';
        $config['parameters']['db'] = $this->getConfigRow(self::DRIVER)['parameters']['db'];
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTrailingSemicolonQuery(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping;";
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    /**
     * @dataProvider configProvider
     */
    public function testUserError(): void
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/pgsql/external_config.yml'));
        $config['parameters']['db'] = $this->dbConfig;
        $config['parameters']['tables'][0]['query'] = "SELECT something, fake";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
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

        $config = $this->getConfigRow(self::DRIVER);

        // queries with comments will break the () in the \copy command.
        // Failed \copy commands should fallback to using the old PDO method
        unset($config['parameters']['table']);
        $config['parameters']['query'] = "
            select * from information_schema.TABLES as tables JOIN (
                -- this is a comment --
                select * from information_schema.columns
            ) as columns ON tables.table_schema = columns.table_schema AND tables.table_name = columns.table_name;
        ";
        $config['parameters']['outputTable'] = 'in.c-main.info_schema';
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        // valid query should not error
        $this->assertContains('Failed \copy command', $process->getOutput());
        // assert that PDO attempt succeeded
        $this->assertEquals(0, $process->getExitCode());
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertJson($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testStateFile(): void
    {
        $outputStateFile = $this->dataDir . '/out/state.json';
        $inputStateFile = $this->dataDir . '/in/state.json';

        $fs = new Filesystem\Filesystem();
        if (!$fs->exists($inputStateFile)) {
            $fs->mkdir($this->dataDir . '/in');
            $fs->touch($inputStateFile);
        }

        // unset the state file
        @unlink($outputStateFile);
        @unlink($inputStateFile);

        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['table']['tableName'] = 'types';
        $config['parameters']['incrementalFetchingColumn'] = 'integer';
        $config['parameters']['outputTable'] = 'in.c-main.types';

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertFileExists($outputStateFile);
        $this->assertFileExists($outputStateFile);
        $this->assertEquals(['lastFetchedRow' => '32'], json_decode(file_get_contents($outputStateFile), true));

        // add a couple rows
        $this->runProcesses([
            $this->createDbProcess('INSERT INTO types ("character", "integer") VALUES (\'abc\', 89), (\'def\', 101)'),
        ]);

        // copy state to input state file
        file_put_contents($inputStateFile, file_get_contents($outputStateFile));

        // run the config again
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals(['lastFetchedRow' => '101'], json_decode(file_get_contents($outputStateFile), true));
    }
}
