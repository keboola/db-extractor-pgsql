<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class PgsqlTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    /** @var  string */
    protected $rootPath;

    /** @var string  */
    protected $dataDir = __DIR__ . '/../../data';

    /** @var  array */
    protected $dbConfig;

    private function createDbProcess(string $query): Process
    {
        return new Process(
            sprintf(
                "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"$query\"",
                $this->dbConfig['#password'],
                $this->dbConfig['host'],
                $this->dbConfig['port'],
                $this->dbConfig['user'],
                $this->dbConfig['database']
            )
        );
    }

    private function runProcesses(array $processes): void
    {
        foreach ($processes as $process) {
            $process->run();
            if (!$process->isSuccessful()) {
                $this->fail($process->getErrorOutput());
            }
        }
    }

    public function setUp(): void
    {
        $this->rootPath = '/code/';
        $config = $this->getConfig();
        $logger = new Logger('ex-db-pgsql-tests');
        $this->app = new Application($config, $logger);

        $this->dbConfig = $config['parameters']['db'];

        // drop test tables
        $processes = [];
        // create a duplicate table in a different schema
        $processes[] = $this->createDbProcess(
            "CREATE SCHEMA IF NOT EXISTS testing;"
        );
        $processes[] = $this->createDbProcess(
            "DROP TABLE IF EXISTS escaping;"
        );
        $processes[] = $this->createDbProcess(
            "DROP TABLE IF EXISTS testing.escaping;"
        );
        $processes[] = $this->createDbProcess(
            "DROP TABLE IF EXISTS types_fk;"
        );
        $processes[] = $this->createDbProcess(
            "DROP TABLE IF EXISTS types;"
        );

        // create test tables
        $processes[] = $this->createDbProcess(
            "CREATE TABLE escaping (" .
            "\"_funnycol\" varchar(123) NOT NULL DEFAULT 'column 1', " .
            "\"_sadcol\" varchar(221) NOT NULL DEFAULT 'column 2', " .
            "PRIMARY KEY (\"_funnycol\", \"_sadcol\"));"
        );

        $processes[] = $this->createDbProcess(
            "\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV;"
        );

        $processes[] = $this->createDbProcess(
            "CREATE TABLE types " .
            "(character varchar(123) PRIMARY KEY, " .
            "integer integer NOT NULL DEFAULT 42, " .
            "decimal decimal(5,3) NOT NULL DEFAULT 1.2, " .
            "date date DEFAULT NULL);"
        );

        $processes[] = $this->createDbProcess(
            "\COPY types FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            "CREATE TABLE types_fk " .
            "(character varchar(123) REFERENCES types (character), " .
            "integer integer NOT NULL DEFAULT 42, " .
            "decimal decimal(5,3) NOT NULL DEFAULT 1.2, " .
            "date date DEFAULT NULL);"
        );

        $processes[] = $this->createDbProcess(
            "\COPY types_fk FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            "CREATE TABLE testing.escaping (" .
            "\"_funnycol\" varchar(123) NOT NULL DEFAULT 'column 1', " .
            "\"_sadcol\" varchar(221) NOT NULL DEFAULT 'column 2', " .
            "PRIMARY KEY (\"_funnY$-col\", \"_sadcol\"));"
        );
        $processes[] = $this->createDbProcess(
            "\COPY testing.escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' "
                . "WITH DELIMITER ',' CSV HEADER;"
        );

        $this->runProcesses($processes);
    }

    public function getConfig(string $driver = 'pgsql', string $format = parent::CONFIG_FORMAT_YAML): array
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'PgSQL';
        return $config;
    }

    /**
     * @param $configType
     * @dataProvider configTypesProvider
     */
    public function testRunConfig(string $configFormat): void
    {
        $config = $this->getConfig('pgsql', $configFormat);
        $result = (new Application($config, new Logger('ex-db-pgsql-tests')))->run();
        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $outputManifest = json_decode(file_get_contents($outputManifestFile), true);

        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['columns']);
        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['primary_key']);
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertContains($expectedArr[$i], $outputArr);
        }
    }

    public function testRunWithSSH(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('pgsql'),
                'public' => $this->getEnv('pgsql', 'DB_SSH_KEY_PUBLIC', true),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
        ];

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));
        $result = $app->run();

        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $this->assertFileExists($outputManifestFile);

        $outputManifest = json_decode(file_get_contents($outputManifestFile), true);
        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['columns']);
        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['primary_key']);

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());


        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertContains($expectedArr[$i], $outputArr);
        }
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = new Application($config, new Logger('ex-db-pgsql-tests'));

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testInvalidCredentialsTestConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['user'] = "fakeguy";
        $app = new Application($config, new Logger('ex-db-pgsql-tests'));

        try {
            $result = $app->run();
            $this->fail("Invalid credentials should throw exception");
        } catch (UserException $exception) {
            $this->assertStringStartsWith("Connection failed", $exception->getMessage());
        }
    }

    public function testInvalidCredentialsAppRun(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['#password'] = "fakepass";

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));
        try {
            $result = $app->run();
            $this->fail("Invalid credentials should throw exception");
        } catch (UserException $exception) {
            $this->assertStringStartsWith("Error connecting", $exception->getMessage());
        }
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config, new Logger('ex-db-pgsql-tests'));

        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertCount(4, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'public',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_funnycol',
                                    'sanitizedName' => 'funnycol',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 123,
                                    'nullable' => false,
                                    'default' => 'column 1',
                                    'ordinalPosition' => 1,
                                ),
                            1 =>
                                array (
                                    'name' => '_sadcol',
                                    'sanitizedName' => 'sadcol',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 221,
                                    'nullable' => false,
                                    'default' => 'column 2',
                                    'ordinalPosition' => 2,
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'types',
                    'schema' => 'public',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'sanitizedName' => 'integer',
                                    'type' => 'integer',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => false,
                                    'default' => '42',
                                    'ordinalPosition' => 2,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'sanitizedName' => 'decimal',
                                    'type' => 'numeric',
                                    'primaryKey' => false,
                                    'length' => '5,3',
                                    'nullable' => false,
                                    'default' => '1.2',
                                    'ordinalPosition' => 3,
                                ),
                            0 =>
                                array (
                                    'name' => 'character',
                                    'sanitizedName' => 'character',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 123,
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => 1,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'sanitizedName' => 'date',
                                    'type' => 'date',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 4,
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'types_fk',
                    'schema' => 'public',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'character',
                                    'sanitizedName' => 'character',
                                    'type' => 'character varying',
                                    'primaryKey' => false,
                                    'length' => 123,
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 1,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'sanitizedName' => 'date',
                                    'type' => 'date',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 4,
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'sanitizedName' => 'integer',
                                    'type' => 'integer',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => false,
                                    'default' => '42',
                                    'ordinalPosition' => 2,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'sanitizedName' => 'decimal',
                                    'type' => 'numeric',
                                    'primaryKey' => false,
                                    'length' => '5,3',
                                    'nullable' => false,
                                    'default' => '1.2',
                                    'ordinalPosition' => 3,
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'testing',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_funnycol',
                                    'sanitizedName' => 'funnycol',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 123,
                                    'nullable' => false,
                                    'default' => 'column 1',
                                    'ordinalPosition' => 1,
                                ),
                            1 =>
                                array (
                                    'name' => '_sadcol',
                                    'sanitizedName' => 'sadcol',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 221,
                                    'nullable' => false,
                                    'default' => 'column 2',
                                    'ordinalPosition' => 2,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testManifestMetadata(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'][3] = $config['parameters']['tables'][2];
        $config['parameters']['tables'][3]['id'] = 4;
        $config['parameters']['tables'][3]['name'] = 'types_fk';
        $config['parameters']['tables'][3]['outputTable'] = 'in.c-main.types_fk';
        $config['parameters']['tables'][3]['primaryKey'] = null;
        $config['parameters']['tables'][3]['table']['tableName'] = 'types_fk';

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));

        $result = $app->run();

        $expectedTableMetadata[0] = [
            0 =>
                [
                    'key' => 'KBC.name',
                    'value' => 'types',
                ],
                1 =>
                [
                    'key' => 'KBC.schema',
                    'value' => 'public',
                ],
                2 =>
                [
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ],
        ];
        $expectedColumnMetadata[0] = array (
            'character' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => 123,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'character',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'character',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                ),
            'integer' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'integer',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '42',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'integer',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'integer',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 2,
                        ),
                ),
            'decimal' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'numeric',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '5,3',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '1.2',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'decimal',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'decimal',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 3,
                        ),
                ),
            'date' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'date',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'DATE',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'date',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'date',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 4,
                        ),
                ),
        );

        $expectedTableMetadata[1] = [
            0 =>
                [
                    'key' => 'KBC.name',
                    'value' => 'types_fk',
                ],
                1 =>
                [
                    'key' => 'KBC.schema',
                    'value' => 'public',
                ],
                2 =>
                [
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ],
        ];
        $expectedColumnMetadata[1] = array (
            'character' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'character varying',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => 123,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'character',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'character',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                ),
            'integer' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'integer',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '42',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'integer',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'integer',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 2,
                        ),
                ),
            'decimal' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'numeric',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '5,3',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '1.2',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'decimal',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'decimal',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 3,
                        ),
                ),
            'date' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'date',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'DATE',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.sourceName',
                            'value' => 'date',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'date',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 4,
                        ),
                ),
        );

        foreach ($result['imported'] as $i => $outputArray) {
            $outputManifest = json_decode(
                file_get_contents($this->dataDir . '/out/tables/' . $outputArray['outputTable'] . '.csv.manifest'),
                true
            );
            $this->assertManifestMetadata($outputManifest, $expectedTableMetadata[$i], $expectedColumnMetadata[$i]);
        }
    }

    protected function assertManifestMetadata(
        array $outputManifest,
        array $expectedTableMetadata,
        array $expectedColumnMetadata
    ): void {
        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $this->assertArrayHasKey('column_metadata', $outputManifest);

        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testTableColumnsQuery(): void
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));

        $result = $app->run();

        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);

        $outputManifest = json_decode(file_get_contents($outputManifestFile), true);
        $this->assertEquals(['character', 'integer', 'decimal', 'date'], $outputManifest['columns']);
        $this->assertEquals(['character'], $outputManifest['primary_key']);

        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertContains($expectedArr[$i], $outputArr);
        }
    }

    public function testColumnOrdering(): void
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]['columns']);

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));
        $result = $app->run();

        $this->assertEquals('success', $result['status']);

        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $outputManifest = json_decode(file_get_contents($outputManifestFile), true);
        // check that the manifest has the correct column ordering
        $this->assertEquals(['character', 'integer', 'decimal', 'date'], $outputManifest['columns']);

        // check the data
        $expectedData = iterator_to_array($expectedCsvFile);
        $outputData = iterator_to_array($outputCsvFile);

        $this->assertCount(4, $outputData);
        foreach ($outputData as $rowNum => $line) {
            // assert timestamp
            $this->assertNotFalse(strtotime($line[3]));
            $this->assertEquals($line[1], $expectedData[$rowNum + 1][1]);
            $this->assertEquals($line[2], $expectedData[$rowNum + 1][2]);
        }
    }

    public function testThousandsOfTablesGetTables(): void
    {
        // $this->markTestSkipped("No need to run this test every time.");
        $testStartTime = time();
        $numberOfSchemas = 10;
        $numberOfTablesPerSchema = 100;
        $numberOfColumnsPerTable = 50;
        $maximumRunTime = 15;

        $processes = [];
        for ($i = 0; $i < $numberOfSchemas; $i++) {
            $processes[] = $this->createDbProcess(sprintf('DROP SCHEMA IF EXISTS testschema_%d CASCADE', $i));
        }

        // gen columns
        $columnsSql = "";
        for ($columnCount = 0; $columnCount < $numberOfColumnsPerTable; $columnCount++) {
            $columnsSql .= sprintf(', "col_%d" VARCHAR(50) NOT NULL DEFAULT \'\'', $columnCount);
        }

        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            $processes[] = $this->createDbProcess(sprintf("CREATE SCHEMA testschema_%d", $schemaCount));
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $processes[] = $this->createDbProcess(
                    sprintf(
                        "CREATE TABLE testschema_%d.testtable_%d (ID SERIAL%s, PRIMARY KEY (ID))",
                        $schemaCount,
                        $tableCount,
                        $columnsSql
                    )
                );
            }
        }
        $this->runProcesses($processes);
        $dbBuildTime = time() - $testStartTime;
        echo "\nTest DB built in  " . $dbBuildTime . " seconds.\n";

        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $jobStartTime = time();
        $result = (new Application($config, new Logger('ex-db-pgsql-tests')))->run();
        $this->assertEquals('success', $result['status']);
        $runTime = time() - $jobStartTime;

        echo "\nThe tables were fetched in " . $runTime . " seconds.\n";
        $this->assertLessThan($maximumRunTime, $runTime);
        $processes = [];
        for ($i = 0; $i < $numberOfSchemas; $i++) {
            $processes[] = $this->createDbProcess(sprintf('DROP SCHEMA IF EXISTS testschema_%d CASCADE', $i));
        }
        $this->runProcesses($processes);
        $entireTime = time() - $testStartTime;
        echo "\nComplete test finished in  " . $entireTime . " seconds.\n";
    }

    public function testBadSprintf(): void
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]['columns']);
        unset($config['parameters']['tables'][2]['table']);
        $config['parameters']['tables'][2]['query'] = "SELECT %s FROM types";

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp("/^Error executing \[types\]\: SQLSTATE\[42601\]\:.*/");

        $app = new Application($config, new Logger('ex-db-pgsql-tests'));
        $app->run();
    }

    public function configTypesProvider(): array
    {
        return [
            [self::CONFIG_FORMAT_YAML],
            [self::CONFIG_FORMAT_JSON],
        ];
    }
}
