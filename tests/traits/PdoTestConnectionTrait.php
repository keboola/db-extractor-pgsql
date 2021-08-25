<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\Extractor\PgSQLConnectionFactory;
use PDO;
use Psr\Log\NullLogger;

trait PdoTestConnectionTrait
{
    use DbConfigTrait;

    public function createTestConnection(bool $ssl = false): PDO
    {
        $factory = new PgSQLConnectionFactory(new NullLogger());
        return $factory->create(self::createDbConfig($ssl))->getConnection();
    }
}
