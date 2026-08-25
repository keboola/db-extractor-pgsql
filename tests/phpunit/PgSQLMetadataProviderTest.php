<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\PgSQLMetadataProvider;
use Keboola\DbExtractor\Tests\Stubs\CapturingPgSQLDbConnection;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class PgSQLMetadataProviderTest extends TestCase
{
    public function testDescriptionsArePropagated(): void
    {
        $connection = $this->createConnection([
            $this->row('users', 'Registered users', 'id', 'Surrogate key', 1),
            $this->row('users', 'Registered users', 'email', null, 2),
        ]);

        $table = (new PgSQLMetadataProvider(new NullLogger(), $connection, true))
            ->listTables()
            ->getByNameAndSchema('users', 'public');

        Assert::assertTrue($table->hasDescription());
        Assert::assertSame('Registered users', $table->getDescription());

        $columns = $table->getColumns();
        Assert::assertSame('Surrogate key', $columns->getByName('id')->getDescription());
        // A column without a COMMENT must not get a description
        Assert::assertFalse($columns->getByName('email')->hasDescription());
    }

    public function testDescriptionsAreNotPropagatedWhenDisabled(): void
    {
        $connection = $this->createConnection([
            $this->row('users', 'Registered users', 'id', 'Surrogate key', 1),
        ]);

        $table = (new PgSQLMetadataProvider(new NullLogger(), $connection, false))
            ->listTables()
            ->getByNameAndSchema('users', 'public');

        Assert::assertFalse($table->hasDescription());
        Assert::assertFalse($table->getColumns()->getByName('id')->hasDescription());
    }

    public function testEmptyCommentIsTreatedAsNoDescription(): void
    {
        $connection = $this->createConnection([
            // Postgres allows COMMENT ON ... IS '' which must not become an empty description
            $this->row('users', '', 'id', '   ', 1),
        ]);

        $table = (new PgSQLMetadataProvider(new NullLogger(), $connection, true))
            ->listTables()
            ->getByNameAndSchema('users', 'public');

        Assert::assertFalse($table->hasDescription());
        Assert::assertFalse($table->getColumns()->getByName('id')->hasDescription());
    }

    public function testQueryReadsCommentsWhenEnabled(): void
    {
        $connection = $this->createConnection([]);
        (new PgSQLMetadataProvider(new NullLogger(), $connection, true))->listTables();

        $sql = $connection->getMetadataQuery();
        Assert::assertStringContainsString('td.description AS table_comment', $sql);
        Assert::assertStringContainsString('cd.description AS column_comment', $sql);
        Assert::assertStringContainsString('pg_catalog.pg_description td', $sql);
        Assert::assertStringContainsString('pg_catalog.pg_description cd', $sql);
    }

    public function testQueryReadsTableCommentOnlyWithoutColumns(): void
    {
        $connection = $this->createConnection([]);
        (new PgSQLMetadataProvider(new NullLogger(), $connection, true))->listTables([], false);

        $sql = $connection->getMetadataQuery();
        Assert::assertStringContainsString('td.description AS table_comment', $sql);
        Assert::assertStringNotContainsString('column_comment', $sql);
    }

    /**
     * The disabled toggle must not change the query at all, so existing
     * configurations keep their exact behaviour.
     *
     * @dataProvider loadColumnsProvider
     */
    public function testQueryIsUntouchedWhenDisabled(bool $loadColumns): void
    {
        $connection = $this->createConnection([]);
        (new PgSQLMetadataProvider(new NullLogger(), $connection, false))
            ->listTables([], $loadColumns);

        $sql = $connection->getMetadataQuery();
        Assert::assertStringNotContainsString('pg_description', $sql);
        Assert::assertStringNotContainsString('description', $sql);
        Assert::assertStringNotContainsString('comment', $sql);
    }

    public function loadColumnsProvider(): array
    {
        return [
            'with columns' => [true],
            'without columns' => [false],
        ];
    }

    private function row(
        string $tableName,
        ?string $tableComment,
        string $columnName,
        ?string $columnComment,
        int $ordinalPosition,
    ): array {
        return [
            'table_schema' => 'public',
            'table_name' => $tableName,
            'table_type' => 'r',
            'table_comment' => $tableComment,
            'column_comment' => $columnComment,
            'column_name' => $columnName,
            'data_type_with_length' => 'integer',
            'nullable' => false,
            'primary_key' => false,
            'ordinal_position' => $ordinalPosition,
            'default_value' => null,
        ];
    }

    /**
     * @param array<array<string, mixed>> $rows
     */
    private function createConnection(array $rows): CapturingPgSQLDbConnection
    {
        return new CapturingPgSQLDbConnection($rows);
    }
}
