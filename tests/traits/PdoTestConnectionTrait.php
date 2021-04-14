<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait PdoTestConnectionTrait
{
    use DbConfigTrait;

    public function createTestConnection(): PDO
    {
        $databaseConfig = $this->createDbConfig();

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
