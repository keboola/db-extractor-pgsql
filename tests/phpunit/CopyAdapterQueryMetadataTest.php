<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Extractor\CopyAdapterQueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\Tests\Stubs\FlakyPgSQLDbConnection;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class CopyAdapterQueryMetadataTest extends TestCase
{
    public function testColumnsAreReadFromMetadataQuery(): void
    {
        $connection = new FlakyPgSQLDbConnection(0, $this->columns('id', 'name'));
        $metadata = new CopyAdapterQueryMetadata($connection, 'SELECT id, name FROM users;');

        Assert::assertSame(['id', 'name'], $metadata->getColumns()->getNames());
        Assert::assertSame(
            ['SELECT * FROM (SELECT id, name FROM users) AS x LIMIT 0'],
            $connection->getQueries(),
        );
        Assert::assertSame(0, $connection->getReconnectCount());
    }

    public function testColumnsAreCachedAfterFirstRead(): void
    {
        $connection = new FlakyPgSQLDbConnection(0, $this->columns('id'));
        $metadata = new CopyAdapterQueryMetadata($connection, 'SELECT id FROM users');

        $metadata->getColumns();
        $metadata->getColumns();

        Assert::assertCount(1, $connection->getQueries());
    }

    public function testDroppedConnectionIsReconnectedAndRetried(): void
    {
        $connection = new FlakyPgSQLDbConnection(1, $this->columns('id', 'name'));
        $metadata = new CopyAdapterQueryMetadata($connection, 'SELECT id, name FROM users');

        Assert::assertSame(['id', 'name'], $metadata->getColumns()->getNames());
        Assert::assertCount(2, $connection->getQueries());
        Assert::assertSame(1, $connection->getReconnectCount());
    }

    public function testPersistentConnectionDropFailsAsUserError(): void
    {
        $connection = new FlakyPgSQLDbConnection(PHP_INT_MAX, $this->columns('id'));
        $metadata = new CopyAdapterQueryMetadata($connection, 'SELECT id FROM users', 3);

        try {
            $metadata->getColumns();
            Assert::fail('A persistent connection error must still fail the job.');
        } catch (UserRetriedException $e) {
            Assert::assertInstanceOf(UserExceptionInterface::class, $e);
            Assert::assertStringContainsString(FlakyPgSQLDbConnection::SSL_EOF_ERROR, $e->getMessage());
        }

        Assert::assertCount(3, $connection->getQueries());
        Assert::assertSame(3, $connection->getReconnectCount());
    }

    private function columns(string ...$names): ColumnCollection
    {
        $columns = [];
        foreach ($names as $name) {
            $columns[] = ColumnBuilder::create()->setName($name)->setType('text')->build();
        }

        return new ColumnCollection($columns);
    }
}
