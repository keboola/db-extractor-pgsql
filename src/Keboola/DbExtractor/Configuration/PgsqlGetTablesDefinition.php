<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class PgsqlGetTablesDefinition extends ActionConfigRowDefinition
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        // @formatter:off
        $rootNode
            ->ignoreExtraKeys(true)
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('driver')->end()
                        ->scalarNode('host')->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('database')
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('user')
                            ->isRequired()
                        ->end()
                        ->scalarNode('#password')
                            ->isRequired()
                        ->end()
                        ->append($this->addSshNode())
                    ->end()
                ->end()
                ->arrayNode('tableListFilter')
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
                    ->end()
                ->end()
            ->end();
        // @formatter:on

        return $treeBuilder;
    }
}
