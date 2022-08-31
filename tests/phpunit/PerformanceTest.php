<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\Component\Logger;
use Keboola\DbExtractor\PgsqlApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\PdoTestConnectionTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use \PDO;
use Psr\Log\Test\TestLogger;

class PerformanceTest extends TestCase
{
    use ConfigTrait;
    use RemoveAllTablesTrait;
    use PdoTestConnectionTrait;

    protected string $dataDir = __DIR__ . '/../data';

    protected PDO $connection;

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createTestConnection();
        $this->removeAllTables();

        $connection = $this->createTestConnection();

        putenv('KBC_DATADIR=' . $this->dataDir);

        $sql = [];
        $sql[] = 'CREATE TABLE IF NOT EXISTS t_0000 (';
        $sql[] = 'id INT,';
        // N cols
        for ($i = 0; $i < 20; $i++) {
            $sql[] = sprintf('"%04d_c" VARCHAR(50) DEFAULT NULL,', $i);
        }

        $sql[] = 'PRIMARY KEY(id)';
        $sql[] = ')';
        $connection->query(implode(' ', $sql));

        // M tables
        for ($i = 1; $i < 300; $i++) {
            $connection->query(sprintf('CREATE TABLE IF NOT EXISTS t_%04d AS TABLE t_0000', $i));
        }
    }

    public function testSpeed(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'] = [];
        $config['action'] = 'getTables';

        $start = microtime(true);
        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        $logger = new TestLogger();

        ob_start();
        $app = new PgsqlApplication($logger);
        $app->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();
        $end = microtime(true);
        $duration = $end-$start;

        echo sprintf('Duration: %.3fs', $duration);
        Assert::assertSame(300, count($result['tables']));
        Assert::assertLessThan(5.0, $duration); // under 5 seconds
    }
}
