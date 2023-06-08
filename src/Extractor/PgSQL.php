<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\FallbackExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Psr\Log\LoggerInterface;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class PgSQL extends BaseExtractor
{
    public const INCREMENTAL_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT', 'TIMESTAMP'];

    private PgSQLDbConnection $connection;

    public function __construct(array $parameters, array $state, LoggerInterface $logger, string $action)
    {
        $parameters['db']['ssh']['compression'] = true;
        parent::__construct($parameters, $state, $logger, $action);
    }

    /**
     * @return PgSQLMetadataProvider
     */
    public function createMetadataProvider(): MetadataProvider
    {
        return new PgSQLMetadataProvider($this->logger, $this->connection);
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $resultWriter = new PgSQLResultWriter($this->state);
        $simpleQueryFactory = new DefaultQueryFactory($this->state);

        $pdoAdapter = new PdoAdapter(
            $this->logger,
            $this->connection,
            $simpleQueryFactory,
            $resultWriter,
            $this->dataDir,
            $this->state
        );

        $copyAdapter = new CopyAdapter(
            $this->connection,
            $this->createDatabaseConfig($this->parameters['db']),
            $simpleQueryFactory,
            $this->getMetadataProvider()
        );

        return new FallbackExportAdapter($this->logger, [
            $copyAdapter,
            $pdoAdapter,
        ]);
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        $factory = new PgSQLConnectionFactory($this->logger);
        $this->connection = $factory->create($databaseConfig);
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
    }

    public function getTables(): array
    {
        $loadColumns = $this->parameters['tableListFilter']['listColumns'] ?? true;
        $loadSystemColumns = $this->parameters['tableListFilter']['includeSystemColumns'] ?? false;
        $whiteList = array_map(
            function (array $table) {
                return new InputTable($table['tableName'], $table['schema']);
            },
            $this->parameters['tableListFilter']['tablesToList'] ?? []
        );

        $tables = $this->getMetadataProvider()->listTables($whiteList, $loadColumns, $loadSystemColumns);
        return $this->getGetTablesSerializer()->serialize($tables);
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this
                ->getMetadataProvider()
                ->getTable($exportConfig->getTable(), true)
                ->getColumns()
                ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        try {
            $datatype = new GenericStorage($column->getType());
            $type = $datatype->getBasetype();
        } catch (InvalidLengthException $e) {
            throw new UserException(
                sprintf(
                    'Column "%s" specified for incremental fetching must has numeric or timestamp type.',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        if (!in_array($type, self::INCREMENTAL_TYPES, true)) {
            throw new UserException(sprintf(
                'Column "%s" specified for incremental fetching has unexpected type "%s", expected: "%s".',
                $exportConfig->getIncrementalFetchingColumn(),
                $datatype->getBasetype(),
                implode('", "', self::INCREMENTAL_TYPES),
            ));
        }
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $result = $this->connection->query(sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
        ), DbConnection::DEFAULT_MAX_RETRIES)->fetchAll();

        return count($result) > 0 ? (string) $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }

    public function getMetadataProvider(): PgSQLMetadataProvider
    {
        /** @var PgSQLMetadataProvider $metadataProvider */
        $metadataProvider = $this->metadataProvider;
        return $metadataProvider;
    }
}
