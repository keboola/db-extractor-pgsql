<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class PgsqlGetTablesListFilterDefinition extends ActionConfigRowDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();

        // @formatter:off
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
                ->arrayNode('tableListFilter')
                    ->children()
                        ->booleanNode('listColumns')->defaultTrue()->end()
                        ->booleanNode('includeSystemColumns')->defaultFalse()->end()
                        ->arrayNode('tablesToList')
                            ->requiresAtLeastOneElement()
                            ->prototype('array')
                                ->children()
                                    ->scalarNode('tableName')->isRequired()->cannotBeEmpty()->end()
                                    ->scalarNode('schema')->isRequired()->cannotBeEmpty()->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
        // @formatter:on

        return $parametersNode;
    }
}
