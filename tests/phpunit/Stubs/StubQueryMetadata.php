<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Stubs;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

/**
 * Returns a fixed set of columns without touching a database.
 */
class StubQueryMetadata implements QueryMetadata
{
    public function __construct(private ColumnCollection $columns)
    {
    }

    public function getColumns(): ColumnCollection
    {
        return $this->columns;
    }
}
