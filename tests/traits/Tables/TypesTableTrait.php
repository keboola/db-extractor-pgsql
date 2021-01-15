<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait TypesTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createTypesTable(string $name = 'types', array $mergeColumns = []): void
    {
        $columns = $this->getTypesColumns();
        if ($mergeColumns) {
            $columns = array_merge($columns, $mergeColumns);
        }
        $this->createTable($name, $columns);
    }

    public function generateTypesRows(string $tableName = 'types'): void
    {
        $data = $this->getTypesRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }


    private function getTypesRows(): array
    {
        // phpcs:disable Generic.Files.LineLength
        return [
            'columns' => ['character', 'integer', 'decimal', 'boolean', 'date'],
            'data' => [
                ['abcdef', '12', '22.400', 'f', '2017-08-05'],
                ['abcdefg', '13', '22.420', 't', '2017-08-06'],
                ['abcdefgh', '32', '22.410', 't', '2017-08-07'],
                ['abcdefghi', '22', '22.440', 't', '2017-08-08'],

            ],
        ];
        // phpcs:enable
    }

    private function getTypesColumns(): array
    {
        return [
            'character' => 'varchar(123) PRIMARY KEY',
            'integer' => 'integer NOT NULL DEFAULT 42',
            'decimal' => 'decimal(5,3) NOT NULL DEFAULT 1.2',
            'boolean' => 'bool NOT NULL DEFAULT FALSE',
            'date' => 'date DEFAULT NULL',
        ];
    }
}
