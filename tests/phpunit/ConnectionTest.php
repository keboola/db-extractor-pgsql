<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\PgSQLConnectionFactory;
use Keboola\DbExtractor\TraitTests\DbConfigTrait;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ConnectionTest extends TestCase
{
    use DbConfigTrait;

    public function testClientEncoding(): void
    {
        $databaseConfig = $this->createDbConfig();
        $factory = new PgSQLConnectionFactory(new NullLogger());
        $connection = $factory->create($databaseConfig);
        Assert::assertSame(['client_encoding' => 'UTF8'], $connection->query(' SHOW CLIENT_ENCODING')->fetch());
    }
}
