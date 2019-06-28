<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\PgsqlConfigRowDefinition;
use Keboola\DbExtractor\Configuration\PgsqlGetTablesDefinition;
use Keboola\DbExtractorLogger\Logger;

class PgsqlApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'PgSQL';

        parent::__construct($config, ($logger) ? $logger : new Logger("ex-db-pgsql"), $state);

        if (!isset($this['parameters']['tables']) && $this['action'] === 'run') {
            // use config definition that allows --forceFallback override
            $this->setConfigDefinition(new PgsqlConfigRowDefinition());
        } else if ($this['action'] === 'getTables') {
            $this->setConfigDefinition(new PgsqlGetTablesDefinition());
        }
    }
}
