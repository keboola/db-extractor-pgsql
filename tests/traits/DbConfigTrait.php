<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

trait DbConfigTrait
{
    public static function getDbConfigArray(bool $ssl = false): array
    {
        $config = [
            'host' => $ssl ? (string) getenv('PGSQL_DB_SSL_HOST') : (string) getenv('PGSQL_DB_HOST'),
            'port' => (string) getenv('PGSQL_DB_PORT'),
            'user' => (string) getenv('PGSQL_DB_USER'),
            '#password' => (string) getenv('PGSQL_DB_PASSWORD'),
            'database' => (string) getenv('PGSQL_DB_DATABASE'),
        ];

        if ($ssl) {
            $config['ssl'] = [
                'enabled' => true,
                'ca' => (string) getenv('SSL_CA'),
                'cert' => (string) getenv('SSL_CERT'),
                '#key' => (string) getenv('SSL_KEY'),
            ];
        }

        return $config;
    }

    public static function createDbConfig(bool $ssl = false): DatabaseConfig
    {
        $dbConfig = self::getDbConfigArray($ssl);
        return DatabaseConfig::fromArray($dbConfig);
    }
}
