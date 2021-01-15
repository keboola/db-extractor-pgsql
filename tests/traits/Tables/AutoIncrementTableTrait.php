<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;
use \PDO;

trait AutoIncrementTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    protected PDO $connection;

    public function createAITable(string $name = 'auto Increment Timestamp'): void
    {
        $this->connection->query('DROP SEQUENCE IF EXISTS user_id_seq;');
        $this->connection->query('CREATE SEQUENCE user_id_seq;');
        $this->createTable($name, $this->getAIColumns());
    }

    public function generateAIRows(string $tableName = 'auto Increment Timestamp'): void
    {
        $data = $this->getAIRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    public function addAIConstraint(string $tableName = 'auto Increment Timestamp'): void
    {
        $this->addConstraint($tableName, 'UNI_KEY_1', 'UNIQUE', '"Weir%d Na-me"');
    }

    private function getAIRows(): array
    {
        return [
            'columns' => ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'datetime'],
            'data' => [
                ['mario', 'plumber', 1, 1.1, '2021-01-05 13:43:17'],
                ['luigi', 'plumber', 2, 2.2, '2021-01-05 13:43:17'],
                ['toad', 'mushroom', 3, 3.3, '2021-01-05 13:43:17'],
                ['princess', 'royalty', 4, 4.4, '2021-01-05 13:43:17'],
                ['wario', 'badguy', 5, 5.5, '2021-01-05 13:43:17'],
                ['yoshi', 'horse?', 6, 6.6, '2021-01-05 13:43:27'],
            ],
        ];
    }

    private function getAIColumns(): array
    {
        return [
            '_Weir%d I-D' => 'INT NOT NULL DEFAULT nextval(\'user_id_seq\')',
            'Weir%d Na-me' => 'VARCHAR(55) NOT NULL DEFAULT \'mario\'',
            'someInteger' => 'INT',
            'someDecimal' => 'DECIMAL(10,2)',
            'type' => 'VARCHAR(55) NULL',
            'datetime' => 'timestamp NOT NULL',
        ];
    }
}
