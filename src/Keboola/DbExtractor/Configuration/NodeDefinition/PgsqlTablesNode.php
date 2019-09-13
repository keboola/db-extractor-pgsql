<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class PgsqlTablesNode extends TablesNode
{
    public const NODE_NAME = 'tableListFilter';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct($parent);

        $this->init();
    }

    protected function init(): void
    {
        // @formatter:off
        $this
            ->prototype('array')
            ->children()
                ->booleanNode('listColumns')->end()
                ->arrayNode('tablesToList')
                ->prototype('array')
                    ->children()
                        ->scalarNode('tableName')->end()
                        ->scalarNode('schema')->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
    }
}
