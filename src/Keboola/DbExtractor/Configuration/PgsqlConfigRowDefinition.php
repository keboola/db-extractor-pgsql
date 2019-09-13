<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class PgsqlConfigRowDefinition extends ConfigRowDefinition
{
    public function __construct(
        ?DbNode $dbNode = null,
        ?SshNode $sshNode = null
    ) {
        parent::__construct($dbNode, $sshNode);
    }

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder();

        /** @var ArrayNodeDefinition $parametersNode */
        $parametersNode = $treeBuilder->root('parameters');
        $this->addValidation($parametersNode);

        // @formatter:off
        $parametersNode
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
                ->integerNode('id')
                    ->min(0)
                ->end()
                ->scalarNode('name')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('query')->end()
                ->arrayNode('table')
                    ->children()
                        ->scalarNode('schema')->isRequired()->end()
                        ->scalarNode('tableName')->isRequired()->end()
                    ->end()
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->scalarNode('incrementalFetchingColumn')->end()
                ->scalarNode('incrementalFetchingLimit')->end()
                ->booleanNode('enabled')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('retries')
                    ->min(0)
                ->end()
                ->booleanNode('forceFallback')
                    ->defaultFalse()
                ->end()
            ->end();
        // @formatter:on

        return $parametersNode;
    }
}
