<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class PgsqlConfigDefinition extends ConfigDefinition
{
    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder();

        /** @var ArrayNodeDefinition $parametersNode */
        $parametersNode = $treeBuilder->root('parameters');

        // @formatter:off
        $parametersNode
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
                ->append($this->dbNodeDefinition)
                ->append($this->tablesNodeDefinition)
            ->end();
        // @formatter:on

        return $parametersNode;
    }
}
