<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class PgsqlSslTest extends BaseTest
{

    /**
     * @dataProvider configProvider
     */
    public function testRunAction(array $config): void
    {
        $this->replaceConfig($config);
        $process = $this->createAppProcess();
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertStringContainsString('sslmode=require', $process->getOutput());
        $this->assertStringContainsString('sslrootcert', $process->getOutput());
        $this->assertStringContainsString('sslcert', $process->getOutput());
        $this->assertStringContainsString('sslkey', $process->getOutput());
    }

    private function replaceConfig(array $config): void
    {
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents('/ssl-cert/ca.crt'),
            'cert' => file_get_contents('/ssl-cert/postgresql.crt'),
            'key' => file_get_contents('/ssl-cert/postgresql.key'),
        ];

        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }
}
