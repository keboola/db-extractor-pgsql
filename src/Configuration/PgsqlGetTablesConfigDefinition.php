<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class PgsqlGetTablesConfigDefinition extends BaseConfigDefinition
{
    /** @var NodeDefinition */
    protected $dbNodeDefinition;

    public function __construct(
        ?DbNode $dbNode = null,
        ?SshNode $sshNode = null
    ) {
        if (is_null($dbNode)) {
            $dbNode = new DbNode($sshNode);
        }
        $this->dbNodeDefinition = $dbNode;
    }

    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder('parameters');

        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();

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
                ->append($this->dbNodeDefinition)
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

        return $rootNode;
    }
}
