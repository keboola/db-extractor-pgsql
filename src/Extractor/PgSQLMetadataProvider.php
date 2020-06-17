<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

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
        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        // Process tables
        $tableRequiredProperties = ['schema', 'type', 'rowCount'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];
        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        foreach ($this->queryTablesAndColumns($whitelist, $loadColumns) as $data) {
            // Table data
            $tableId = $data['table_schema'] . '.' . $data['table_name'];
            if (!array_key_exists($tableId, $tableBuilders)) {
                $tableBuilder = $builder->addTable();
                $tableBuilders[$tableId] = $tableBuilder;
                $tableBuilder
                    ->setName((string) $data['table_name'])
                    ->setSchema($data['table_schema'] ?? '')
                    ->setType(self::tableTypeFromCode($data['table_type']));
            } else {
                $tableBuilder = $tableBuilders[$tableId];
            }

            // Column data
            $columnType = $data['data_type_with_length'];
            $length = null;

            // Length
            if (preg_match('/(.*)\((\d+|\d+,\d+)\)/', $columnType, $parsedType) === 1) {
                $columnType = $parsedType[1] ?? null;
                $length = $parsedType[2] ?? null;
            }

            // Default value
            $default = $data['default_value'];
            if ($columnType === 'character varying' && $default !== null) {
                $default = str_replace("'", '', explode('::', $data['default_value'])[0]);
            }

            // Create  column
            $tableBuilder
                ->addColumn()
                ->setName($data['column_name'])
                ->setType($columnType)
                ->setPrimaryKey($data['primary_key'] ?: false)
                ->setLength($length)
                ->setNullable($data['nullable'])
                ->setDefault($default)
                ->setOrdinalPosition($data['ordinal_position']);
        }

        return $builder->build();
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
        $sql[] = 'INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace';

        if ($loadColumns) {
            // Indexes have 0 reltype, we don't want them here
            $sql[] = 'JOIN pg_class c ON a.attrelid = c.oid AND c.reltype != 0';

            // PKs
            $sql[] = 'LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)';

            // Default values
            $sql[] = 'LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)';
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
