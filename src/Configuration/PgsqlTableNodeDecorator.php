<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;

class PgsqlTableNodeDecorator extends TableNodesDecorator
{
    public function normalize(array $v): array
    {
        // Fix BC: some old configs can contain limit but not column
        if (!empty($v['incrementalFetchingLimit']) && empty($v['incrementalFetchingColumn'])) {
            unset($v['incrementalFetchingLimit']);
        }

        return parent::normalize($v);
    }
}
