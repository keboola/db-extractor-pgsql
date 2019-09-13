<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\PgsqlTablesNode;
use Keboola\DbExtractor\Configuration\PgsqlConfigDefinition;
use Keboola\DbExtractor\Configuration\PgsqlConfigRowDefinition;
use Keboola\DbExtractorConfig\Config;
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
        if (!isset($this['parameters']['tables']) && $this['action'] === 'run') {
            $this->config = new Config(
                $config,
                new PgsqlConfigRowDefinition()
            );
        } else if ($this['action'] === 'getTables') {
            $this->config = new Config(
                $config,
                new PgsqlConfigDefinition(null, null, new PgsqlTablesNode())
            );
        } else {
            parent::buildConfig($config);
        }
    }
}
