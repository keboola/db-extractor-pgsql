<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\FallbackExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use PDO;

class PgSQL extends BaseExtractor
{
    public const INCREMENTAL_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT', 'TIMESTAMP'];

    private PDOAdapter $pdoAdapter;

    private CopyAdapter $copyAdapter;

    private PgSQLDbConnection $connection;

    private ?PgSQLMetadataProvider $metadataProvider = null;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $parameters['db']['ssh']['compression'] = true;
        parent::__construct($parameters, $state, $logger);
    }

    /**
     * @return PgSQLMetadataProvider
     */
    public function getMetadataProvider(): MetadataProvider
    {
        if (!$this->metadataProvider) {
            $this->metadataProvider = new PgSQLMetadataProvider($this->logger, $this->connection);
        }
        return $this->metadataProvider;
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $resultWriter = new PgSQLResultWriter($this->state);
        $simpleQueryFactory = new DefaultQueryFactory($this->state);

        $this->pdoAdapter = new PDOAdapter(
            $this->logger,
            $this->connection,
            $simpleQueryFactory,
            $resultWriter,
            $this->dataDir,
            $this->state
        );

        $this->copyAdapter = new CopyAdapter(
            $this->logger,
            $this->connection,
            $this->createDatabaseConfig($this->parameters['db']),
            $simpleQueryFactory,
            $this->getMetadataProvider()
        );

        return new FallbackExportAdapter($this->logger, [
            $this->copyAdapter,
            $this->pdoAdapter,
        ]);
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 60,
        ];

        $port = $databaseConfig->hasPort() ? $databaseConfig->getPort() : '5432';

        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;',
            $databaseConfig->getHost(),
            $port,
            $databaseConfig->getDatabase()
        );

        if ($databaseConfig->hasSSLConnection()) {
            $dsn .= 'sslmode=require;';
            $tempDir = new Temp('ssl');
            $sslConnection = $databaseConfig->getSslConnectionConfig();

            if ($sslConnection->hasCa()) {
                $dsn .= sprintf(
                    'sslrootcert="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getCa())
                );
            }

            if ($sslConnection->hasCert()) {
                $dsn .= sprintf(
                    'sslcert="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getCert())
                );
            }

            if ($sslConnection->hasKey()) {
                $dsn .= sprintf(
                    'sslkey="%s";',
                    SslHelper::createSSLFile($tempDir, $sslConnection->getKey())
                );
            }
        }

        $this->connection = new PgSQLDbConnection(
            $this->logger,
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $options
        );
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
        $this->copyAdapter->testConnection();
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this
                ->getMetadataProvider()
                ->getTable($exportConfig->getTable())
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
}
