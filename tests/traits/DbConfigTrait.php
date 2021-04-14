<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

trait DbConfigTrait
{
    public function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('PGSQL_DB_HOST'),
            'port' => (string) getenv('PGSQL_DB_PORT'),
            'user' => (string) getenv('PGSQL_DB_USER'),
            '#password' => (string) getenv('PGSQL_DB_PASSWORD'),
            'database' => (string) getenv('PGSQL_DB_DATABASE'),
        ];
    }

    public function createDbConfig(): DatabaseConfig
    {
        $dbConfig = $this->getDbConfigArray();
        return DatabaseConfig::fromArray($dbConfig);
    }
}
