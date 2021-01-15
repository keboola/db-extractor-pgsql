<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use \Throwable;

class DatadirTest extends DatadirTestCase
{
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    protected PDO $connection;

    protected string $testProjectDir;

    protected string $testTempDir;

    public function getConnection(): PDO
    {
        return $this->connection;
    }

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        putenv('SSH_PRIVATE_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa.pub'));
    }

    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $this->prettifyAllManifests($actual);
        parent::assertDirectoryContentsSame($expected, $actual);
    }

    protected function modifyConfigJsonContent(string $content): string
    {
        $config = json_decode($content, true, 512, JSON_THROW_ON_ERROR);

        $sslCa = $config['parameters']['db']['ssl']['ca'] ?? null;
        $sslCert = $config['parameters']['db']['ssl']['cert'] ?? null;
        $sslKey = $config['parameters']['db']['ssl']['key'] ?? null;

        if ($sslCa) {
            $config['parameters']['db']['ssl']['ca'] = $this->getCert($sslCa);
        }
        if ($sslCert) {
            $config['parameters']['db']['ssl']['cert'] = $this->getCert($sslCert);
        }
        if ($sslKey) {
            $config['parameters']['db']['ssl']['key'] = $this->getCert($sslKey);
        }

        return parent::modifyConfigJsonContent((string) json_encode($config));
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $this->connection = PdoTestConnection::createConnection();
        $this->removeAllTables();
        $this->closeSshTunnels();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $fs = new Filesystem();
        if ($fs->exists('/usr/local/share/ca-certificates/mssql.crt')) {
            $fs->remove('/usr/local/share/ca-certificates/mssql.crt');
            Process::fromShellCommandline('update-ca-certificates --fresh')->mustRun();
        }
    }

    protected function prettifyAllManifests(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $this->prettifyJsonFile((string) $file->getRealPath());
        }
    }

    protected function prettifyJsonFile(string $path): void
    {
        $json = (string) file_get_contents($path);
        try {
            file_put_contents($path, (string) json_encode(json_decode($json), JSON_PRETTY_PRINT));
        } catch (Throwable $e) {
            // If a problem occurs, preserve the original contents
            file_put_contents($path, $json);
        }
    }

    protected function findManifests(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->name(['~.*\.manifest~']);
    }

    private function getCert(string $filename): string
    {
        $filepath = sprintf('/ssl-cert/%s', $filename);
        if (file_exists($filepath)) {
            return (string) file_get_contents($filepath);
        }
        return $filename;
    }
}
