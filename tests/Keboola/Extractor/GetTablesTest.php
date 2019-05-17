<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;
use Keboola\DbExtractor\Exception\UserException;

class GetTablesTest extends BaseTest
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

    public function testGetTables(): void
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $data = json_decode($process->getOutput(), true);
        self::assertCount(4, $data['tables']);
        self::assertArrayHasKey('columns', $data['tables'][0]);
        self::assertEquals(0, $process->getExitCode());
        self::assertEquals("", $process->getErrorOutput());
    }

    public function testGetTablesNoColumns(): void
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => false,
        ];
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $data = json_decode($process->getOutput(), true);
        self::assertCount(4, $data['tables']);
        self::assertArrayNotHasKey('columns', $data['tables'][0]);
        self::assertEquals(0, $process->getExitCode());
        self::assertEquals("", $process->getErrorOutput());
    }

    public function testGetTablesOneTable(): void
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'tablesToList' => [[
                'tableName' => 'types_fk',
                'schema' => 'public',
            ]],
        ];
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $data = json_decode($process->getOutput(), true);
        self::assertCount(1, $data['tables']);
        self::assertEquals('types_fk', $data['tables'][0]['name']);
        self::assertArrayHasKey('columns', $data['tables'][0]);
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testGetTablesOneTableNoColumns(): void
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => false,
            'tablesToList' => [[
                'tableName' => 'types_fk',
                'schema' => 'public',
            ]],
        ];
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = Process::fromShellCommandline('php ' . $this->rootPath . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
        $data = json_decode($process->getOutput(), true);
        self::assertCount(1, $data['tables']);
        self::assertEquals('types_fk', $data['tables'][0]['name']);
        self::assertArrayNotHasKey('columns', $data['tables'][0]);
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }
}