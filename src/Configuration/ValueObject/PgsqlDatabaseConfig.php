<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use Keboola\DbExtractor\Extractor\CursorQueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;

class PgsqlDatabaseConfig extends DatabaseConfig
{
    private int $batchSize;

    public static function fromArray(array $data): self
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'],
            isset($data['port']) ? (string) $data['port'] : null,
            $data['user'],
            $data['#password'],
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['initQueries'] ?? [],
            $data['batchSize'] ?? CursorQueryResult::DEFAULT_BATCH_SIZE,
        );
    }

    public function __construct(
        string $host,
        ?string $port,
        string $username,
        string $password,
        ?string $database,
        ?string $schema,
        ?SSLConnectionConfig $sslConnectionConfig,
        array $initQueries,
        int $batchSize,
    ) {
        $this->batchSize = $batchSize;

        parent::__construct($host, $port, $username, $password, $database, $schema, $sslConnectionConfig, $initQueries);
    }

    public function getBatchSize(): int
    {
        return $this->batchSize;
    }
}
