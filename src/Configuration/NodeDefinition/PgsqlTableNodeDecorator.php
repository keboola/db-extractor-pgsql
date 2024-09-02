<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

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

    protected function addPrimaryKeyNode(NodeBuilder $builder): void
    {
        // @formatter:off
        // Fix BC: some old configs can be empty primary key
        $builder
            ->arrayNode('primaryKey')
                ->beforeNormalization()
                    ->ifArray()
                    ->then(function ($v) {
                        return array_filter($v, function ($value) {
                            return $value !== '';
                        });
                    })
                ->end()
                ->prototype('scalar')->end();
        // @formatter:on
    }
}
