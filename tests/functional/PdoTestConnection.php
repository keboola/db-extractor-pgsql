<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\Extractor\SslHelper;
use Keboola\Temp\Temp;
use PDO;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

class PdoTestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('PGSQL_DB_HOST'),
            'port' => (string) getenv('PGSQL_DB_PORT'),
            'user' => (string) getenv('PGSQL_DB_USER'),
            '#password' => (string) getenv('PGSQL_DB_PASSWORD'),
            'database' => (string) getenv('PGSQL_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(): DatabaseConfig
    {
        $dbConfig = self::getDbConfigArray();
        return DatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(): PDO
    {
        $databaseConfig = self::createDbConfig();

        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 60,
        ];

        $port = $databaseConfig->hasPort() ? $databaseConfig->getPort() : '5432';

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;',
            $databaseConfig->getHost(),
            $port,
            $databaseConfig->getDatabase()
        );

        $pdo = new PDO(
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $options
        );
        $pdo->exec("SET NAMES 'UTF8';");

        return $pdo;
    }
}
