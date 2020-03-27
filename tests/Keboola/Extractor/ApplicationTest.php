<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Filesystem;
use Symfony\Component\Process\Process;
use Keboola\DbExtractor\Exception\UserException;

class ApplicationTest extends BaseTest
{
    private function replaceConfig(array $config): void
    {
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }

    /**
     * @dataProvider configProvider
     */
    public function testRunAction(array $config): void
    {
        $this->replaceConfig($config);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testRunActionSshTunnel(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => self::DRIVER,
            'remotePort' => '1433',
            'localPort' => '1234',
        ];
        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString(
            'Creating SSH tunnel to \'sshproxy\' on local port \'1234\'',
            $process->getOutput()
        );
        $this->assertStringContainsString('host=127.0.0.1;port=1234', $process->getOutput());
    }

    public function testTestConnectionAction(): void
    {
        $config['action'] = 'testConnection';
        $config['parameters']['db'] = $this->getConfigRow(self::DRIVER)['parameters']['db'];
        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testTrailingSemicolonQuery(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM escaping;';
        $this->replaceConfig($config);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testUserError(): void
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/pgsql/external_config.json'), true);
        $config['parameters']['db'] = $this->dbConfig;
        $config['parameters']['tables'][0]['query'] = 'SELECT something, fake';
        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertFalse(strstr($process->getErrorOutput(), 'PGPASSWORD'));
        $this->assertStringContainsString($config['parameters']['tables'][0]['name'], $process->getErrorOutput());
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
        $config['parameters']['query'] = '
            select * from information_schema.TABLES as tables JOIN (
                -- this is a comment --
                select * from information_schema.columns
            ) as columns ON tables.table_schema = columns.table_schema AND tables.table_name = columns.table_name;
        ';
        $config['parameters']['outputTable'] = 'in.c-main.info_schema';
        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        // valid query should not error
        $this->assertStringContainsString('Failed \copy command', $process->getOutput());
        // assert that PDO attempt succeeded
        $this->assertEquals(0, $process->getExitCode());
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
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

        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertFileExists($outputStateFile);
        $this->assertFileExists($outputStateFile);
        $this->assertEquals(['lastFetchedRow' => '32'], json_decode(
            (string) file_get_contents($outputStateFile),
            true
        ));

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
        $this->assertEquals(['lastFetchedRow' => '101'], json_decode(
            (string) file_get_contents($outputStateFile),
            true
        ));
    }

    public function testGetTablesNonConfigRowConfig(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';
        $this->replaceConfig($config);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertJson($process->getOutput());
    }

    public function testBoolConsistency(): void
    {
        $config = $this->getConfigRow(self::DRIVER);

        $config['parameters']['table']['tableName'] = 'types';
        $config['parameters']['forceFallback'] = true;
        $config['parameters']['useConsistentFallbackBooleanStyle'] = true;
        $config['parameters']['outputTable'] = 'in.c-main.bool_consistency_test';
        $this->replaceConfig($config);

        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/in.c-main.bool_consistency_test.csv');

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        // assert the correct file contents
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertContains($expectedArr[$i], $outputArr);
        }
    }

    public function testExportEmptyTableData(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['table']['tableName'] = 'empty_table';
        $config['parameters']['outputTable'] = 'empty_table';
        $this->replaceConfig($config);
        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $this->assertStringContainsString(
            'Query returned empty result. Nothing was imported to [empty_table]',
            $process->getErrorOutput()
        );
        $this->assertFileNotExists($this->dataDir . '/out/tables/empty_table.csv');
    }
}
