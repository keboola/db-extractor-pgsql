<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\IncrementalFetchingConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class PgsqlExportConfig extends ExportConfig
{
    /** If true, then the \copy command will not be used but the PDO fallback directly */
    private bool $forceFallback;

    /** If true, then 1/0 booleans values will be replaced by T/F when PDO fallback is used */
    private bool $replaceBooleans;

    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'] ?? null,
            $data['name'] ?? null,
            $data['query'],
            empty($data['query']) ? InputTable::fromArray($data) : null,
            $data['incremental'] ?? false,
            empty($data['query']) ? IncrementalFetchingConfig::fromArray($data) : null,
            $data['columns'],
            $data['outputTable'],
            $data['primaryKey'],
            $data['retries'],
            // Only in the config row configuration
            $data['forceFallback'] ?? false,
            $data['useConsistentFallbackBooleanStyle'] ?? false,
        );
    }

    public function __construct(
        ?int $configId,
        ?string $configName,
        ?string $query,
        ?InputTable $table,
        bool $incrementalLoading,
        ?IncrementalFetchingConfig $incrementalFetchingConfig,
        array $columns,
        string $outputTable,
        array $primaryKey,
        int $maxRetries,
        bool $forceFallback,
        bool $replaceBooleans,
    ) {
        parent::__construct(
            $configId,
            $configName,
            $query,
            $table,
            $incrementalLoading,
            $incrementalFetchingConfig,
            $columns,
            $outputTable,
            $primaryKey,
            $maxRetries,
        );
        $this->forceFallback = $forceFallback;
        $this->replaceBooleans = $replaceBooleans;
    }

    public function getForceFallback(): bool
    {
        return $this->forceFallback;
    }

    public function getReplaceBooleans(): bool
    {
        return $this->replaceBooleans;
    }
}
