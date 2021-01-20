<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

trait QuoteIdentifierTrait
{

    public function quoteIdentifier(string $str): string
    {
        return sprintf('"%s"', $str);
    }
}
