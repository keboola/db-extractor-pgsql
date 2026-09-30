<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

/**
 * A table carrying COMMENT ON TABLE / COMMENT ON COLUMN values.
 *
 * Deliberately a dedicated table rather than a comment added to one of the
 * shared fixtures -- commenting those would change the expected manifests of
 * every other functional test.
 */
trait CommentsTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createCommentsTable(string $name = 'comments'): void
    {
        $this->createTable($name, $this->getCommentsColumns());
    }

    public function addCommentsToTable(string $name = 'comments'): void
    {
        $statements = [
            sprintf('COMMENT ON TABLE %s IS %s', $this->quoteIdentifier($name), "'Table level comment'"),
            sprintf('COMMENT ON COLUMN %s.id IS %s', $this->quoteIdentifier($name), "'Surrogate key'"),
            sprintf('COMMENT ON COLUMN %s.name IS %s', $this->quoteIdentifier($name), "'Customer name'"),
            // "note" is intentionally left without a comment.
            // Postgres discards an empty comment rather than storing an empty
            // string, so this column ends up identical to "note" -- it guards
            // against COMMENT ON ... IS '' being reported as a description.
            sprintf('COMMENT ON COLUMN %s.empty_comment IS %s', $this->quoteIdentifier($name), "''"),
        ];

        foreach ($statements as $statement) {
            $this->connection->prepare($statement)->execute();
        }
    }

    public function generateCommentsRows(string $tableName = 'comments'): void
    {
        $data = $this->getCommentsRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getCommentsRows(): array
    {
        return [
            'columns' => ['id', 'name', 'note', 'empty_comment'],
            'data' => [
                [1, 'Alice', 'first', 'x'],
                [2, 'Bob', 'second', 'y'],
            ],
        ];
    }

    private function getCommentsColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            'name' => 'VARCHAR(100)',
            'note' => 'VARCHAR(100)',
            'empty_comment' => 'VARCHAR(100)',
        ];
    }
}
