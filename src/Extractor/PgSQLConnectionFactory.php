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

        $dsn = 'pgsql:' . PgSQLDsnFactory::createForPdo($databaseConfig);
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
