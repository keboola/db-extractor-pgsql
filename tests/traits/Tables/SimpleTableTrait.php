<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;

trait SimpleTableTrait
{
    use CreateTableTrait;

    public function createSimpleTable(string $name = 'simple'): void
    {
        $this->createTable($name, $this->getSimpleColumns());
    }

    public function addSimpleConstraint(string $tableName = 'simple'): void
    {
        $this->addConstraint($tableName, 'PK_AUTOINC', 'PRIMARY KEY', 'id');
    }

    protected function getSimpleColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            'name' => 'VARCHAR(100)',
        ];
    }
}
