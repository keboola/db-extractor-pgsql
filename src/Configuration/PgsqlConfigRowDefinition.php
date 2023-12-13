<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class PgsqlConfigRowDefinition extends ConfigRowDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $rootNode = parent::getParametersDefinition();

        // @formatter:off
        $rootNode
            ->children()
                ->booleanNode('useConsistentFallbackBooleanStyle')
                    ->defaultFalse()
                ->end()
                ->booleanNode('forceFallback')
                    ->defaultFalse()
                ->end()
                ->integerNode('batchSize')->end()
            ->end();
        // @formatter:on

        return $rootNode;
    }
}
