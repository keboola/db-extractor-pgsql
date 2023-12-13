<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\PgsqlTableNodeDecorator;
use Keboola\DbExtractor\Configuration\PgsqlConfigRowDefinition;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\Configuration\PgsqlGetTablesListFilterDefinition;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class PgsqlApplication extends Application
{
    protected function loadConfig(): void
    {
        $config = $this->getRawConfig();
        $action = $config['action'] ?? 'run';

        $config['parameters']['extractor_class'] = 'PgSQL';
        $config['parameters']['data_dir'] = $this->getDataDir();

        if ($action === 'getTables') {
            $this->config = new Config($config, new PgsqlGetTablesListFilterDefinition());
        } elseif ($this->isRowConfiguration($config)) {
            if ($action === 'run') {
                $this->config = new Config(
                    $config,
                    new PgsqlConfigRowDefinition(null, null, null, new PgsqlTableNodeDecorator()),
                );
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition());
            }
        } else {
            $this->config = new Config(
                $config,
                new ConfigDefinition(null, null, null, new PgsqlTableNodeDecorator()),
            );
        }
    }

    protected function createExportConfig(array $data): ExportConfig
    {
        return PgsqlExportConfig::fromArray($data);
    }
}
