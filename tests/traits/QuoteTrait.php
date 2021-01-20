<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

trait QuoteTrait
{

    public function quote(string $str): string
    {
        return sprintf("'%s'", $str);
    }
}
