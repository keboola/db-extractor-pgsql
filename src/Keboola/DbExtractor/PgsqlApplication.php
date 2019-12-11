<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\PgsqlGetTablesConfigDefinition;
use Keboola\DbExtractor\Configuration\PgsqlConfigRowDefinition;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorLogger\Logger;

class PgsqlApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'PgSQL';

        parent::__construct($config, ($logger) ? $logger : new Logger('ex-db-pgsql'), $state);
    }

    public function buildConfig(array $config): void
    {
        if ($this['action'] === 'getTables') {
            $this->config = new Config($config, new PgsqlGetTablesConfigDefinition());
        } elseif ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
                $this->config = new Config($config, new PgsqlConfigRowDefinition());
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition());
            }
        } else {
            $this->config = new Config($config, new ConfigDefinition());
        }
    }
}
