<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use PDO;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class PgSQLConnectionFactory
{
    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function create(DatabaseConfig $databaseConfig): PgSQLDbConnection
    {
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

        if ($databaseConfig->hasSSLConnection()) {
            $dsn .= 'sslmode=require;';
            $tempDir = new Temp('ssl');
            $sslConnection = $databaseConfig->getSslConnectionConfig();

            if ($sslConnection->hasCa()) {
                $dsn .= sprintf(
                    'sslrootcert="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getCa())
                );
            }

            if ($sslConnection->hasCert()) {
                $dsn .= sprintf(
                    'sslcert="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getCert())
                );
            }

            if ($sslConnection->hasKey()) {
                $dsn .= sprintf(
                    'sslkey="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getKey())
                );
            }
        }

        return new PgSQLDbConnection(
            $this->logger,
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $options,
            function (PDO $pdo): void {
                $pdo->query("SET NAMES 'UTF8';");
            }
        );
    }
}
