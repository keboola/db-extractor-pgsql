<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Psr\Log\LoggerInterface;

class PgSQLMetadataProvider implements MetadataProvider
{
    private LoggerInterface $logger;

    private PDOAdapter $pdoAdapter;

    /** @var TableCollection[] */
    private array $cache = [];

    public function __construct(LoggerInterface $logger, PDOAdapter $pdoAdapter)
    {
        $this->logger = $logger;
        $this->pdoAdapter = $pdoAdapter;
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        // Return cached value if present
        $cacheKey = md5(serialize(func_get_args()));
        if (isset($this->cache[$cacheKey])) {
            return $this->cache[$cacheKey];
        }

        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        /** @var TableBuilder[] $columnBuilders */
        $columnBuilders = [];

        // Process tables
        $tableRequiredProperties = ['schema'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];
        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        foreach ($this->queryTablesAndColumns($whitelist, $loadColumns) as $data) {
            // Table data
            $tableId = $data['table_schema'] . '.' . $data['table_name'];
            if (!array_key_exists($tableId, $tableBuilders)) {
                $tableBuilder = $this->processTable($data, $builder);
                $tableBuilders[$tableId] = $tableBuilder;
            } else {
                $tableBuilder = $tableBuilders[$tableId];
            }

            if ($loadColumns) {
                $columnId = $data['table_schema'] . '.' . $data['table_name'] . '.' . $data['column_name'];
                if (!array_key_exists($columnId, $columnBuilders)) {
                    $columnBuilders[$columnId] = $this->processColumn($data, $tableBuilder);
                }
            } else {
                $tableBuilder->setColumnsNotExpected();
            }
        }

        $collection = $builder->build();
        $this->cache[$cacheKey] = $collection;
        return $collection;
    }

    private function processTable(array $data, MetadataBuilder $builder): TableBuilder
    {
        $table = $builder
            ->addTable()
            ->setName((string) $data['table_name'])
            ->setSchema($data['table_schema'] ?? '');

        $type = self::tableTypeFromCode($data['table_type']);
        if ($type) {
            $table->setType($type);
        }

        return $table;
    }

    private function processColumn(array $data, TableBuilder $tableBuilder): ColumnBuilder
    {
        // Column type
        $columnType = $data['data_type_with_length'];

        // Length
        $length = null;
        if (preg_match('/(.*)\((\d+|\d+,\d+)\)/', $columnType, $parsedType) === 1) {
            $columnType = $parsedType[1] ?? null;
            $length = $parsedType[2] ?? null;
        }

        // Create  column
        $columnBuilder = $tableBuilder
            ->addColumn()
            ->setName($data['column_name'])
            ->setType($columnType)
            ->setPrimaryKey($data['primary_key'] ?: false)
            ->setLength($length)
            ->setNullable($data['nullable'])
            ->setOrdinalPosition($data['ordinal_position']);

        // Default value
        $default = $columnType === 'character varying' && $data['default_value'] !== null ?
            str_replace("'", '', explode('::', $data['default_value'])[0]) :
            $data['default_value'];

        if ($default) {
            $columnBuilder->setDefault($default);
        }

        return $columnBuilder;
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryTablesAndColumns(array $whitelist, bool $loadColumns): iterable
    {
        $sql = [];

        // Select --------
        $select = [];
        $select[] = 'ns.nspname AS table_schema';
        $select[] = 'c.relname AS table_name';
        $select[] = 'c.relkind AS table_type';

        if ($loadColumns) {
            $select[] = 'a.attname AS column_name';
            $select[] = 'format_type(a.atttypid, a.atttypmod) AS data_type_with_length';
            $select[] = 'NOT a.attnotnull AS nullable';
            $select[] = 'i.indisprimary AS primary_key';
            $select[] = 'a.attnum AS ordinal_position';

            // Default value
            $version = $this->getDbServerVersion();
            $defaultValueStatement = $version < 120000 ? 'd.adsrc' : 'pg_get_expr(d.adbin::pg_node_tree, d.adrelid)';
            $select[] = "$defaultValueStatement AS default_value";
        }
        $sql[] = 'SELECT ' . implode(', ', $select);

        // From --------
        $sql[] = $loadColumns ? 'FROM pg_attribute a' : 'FROM pg_class c' ;

        // Joins --------
        // Schema

        if ($loadColumns) {
            // Indexes have 0 reltype, we don't want them here
            $sql[] = 'JOIN pg_class c ON a.attrelid = c.oid AND c.reltype != 0';
            $sql[] = 'INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace';

            // PKs
            $sql[] = 'LEFT JOIN pg_index i ON';
            $sql[] = 'a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary = TRUE';

            // Default values
            $sql[] = 'LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)';
        } else {
            $sql[] = 'INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace';
        }

        // Where --------
        $where = [];
        // Exclude system namespaces
        $where[] = "ns.nspname != 'information_schema'";
        $where[] = "ns.nspname != 'pg_catalog'";
        $where[] = "ns.nspname NOT LIKE 'pg_toast%'";
        $where[] = "ns.nspname NOT LIKE 'pg_temp%'";

        if ($loadColumns) {
            // Exclude dropped columns
            $where[] = 'NOT a.attisdropped';

            // Exclude system columns
            $where[] = 'a.attnum > 0';
        } else {
            $where[] = "c.relkind IN ('r', 'S', 't', 'v', 'm', 'f', 'p')";
        }

        if ($whitelist) {
            $where[] = sprintf(
                'c.relname IN (%s) AND ns.nspname IN (%s)',
                implode(
                    ',',
                    array_map(fn (InputTable $table) => $this->pdoAdapter->quote($table->getName()), $whitelist)
                ),
                implode(
                    ',',
                    array_map(fn (InputTable $table) => $this->pdoAdapter->quote($table->getSchema()), $whitelist)
                )
            );
        }
        $sql[] = 'WHERE ' . implode(' AND ', $where);

        // Sort
        $sql[] = $loadColumns ?
            'ORDER BY table_schema, table_name, ordinal_position' : 'ORDER BY table_schema, table_name';

        // Run query
        return $this->pdoAdapter->runRetryableQuery(implode(' ', $sql));
    }


    private function getDbServerVersion(): int
    {
        $sqlGetVersion = 'SHOW server_version_num;';
        $version = $this->pdoAdapter->runRetryableQuery($sqlGetVersion);
        $this->logger->info(
            sprintf('Found database server version: %s', $version[0]['server_version_num'])
        );
        return (int) $version[0]['server_version_num'];
    }

    private static function tableTypeFromCode(string $code): string
    {
        switch ($code) {
            case 'r':
                return 'table';
            case 'v':
                return 'view';
            case 'm':
                return 'materialized view';
            case 'f':
                return 'foreign table';
            case 'i':
                return 'index';
            case 'S':
                return 'sequence';
            case 'c':
                return 'composite type';
            case 't':
                return 'toast table';
            default:
                return '';
        }
    }
}
