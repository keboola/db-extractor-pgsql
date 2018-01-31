<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class PgsqlTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    protected $rootPath;

    private function createDbProcess($dbConfig, $query)
    {
        return new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"$query\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
    }

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-pgsql');
        }
        if (!defined('ROOT_PATH')) {
            define('ROOT_PATH', '/code/');
        }
        $this->rootPath = getenv('ROOT_PATH') ? getenv('ROOT_PATH') : '/code/';
        $config = $this->getConfig();
        $this->app = new Application($config);

        $dbConfig = $config['parameters']['db'];

        // drop test tables
        $processes = [];
        $processes[] = $this->createDbProcess(
            $dbConfig,
            "DROP TABLE IF EXISTS escaping;"
        );
        $processes[] = $this->createDbProcess(
            $dbConfig,
            "DROP TABLE IF EXISTS types_fk;"
        );
        $processes[] = $this->createDbProcess(
            $dbConfig,
            "DROP TABLE IF EXISTS types;"
        );

        // create test tables
        $processes[] = $this->createDbProcess(
            $dbConfig,
            "CREATE TABLE escaping (col1 varchar(123) NOT NULL DEFAULT 'column 1', col2 varchar(221) NOT NULL DEFAULT 'column 2', PRIMARY KEY (col1, col2));"
        );


        $processes[] = $this->createDbProcess(
            $dbConfig,
            "\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            $dbConfig,
            "CREATE TABLE types " .
                    "(character varchar(123) PRIMARY KEY, " .
                    "integer integer NOT NULL DEFAULT 42, " .
                    "decimal decimal(5,3) NOT NULL DEFAULT 1.2, " .
                    "date date DEFAULT NULL);"
        );

        $processes[] = $this->createDbProcess(
            $dbConfig,
            "\COPY types FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );

        $processes[] = $this->createDbProcess(
            $dbConfig,
            "CREATE TABLE types_fk " .
            "(character varchar(123) REFERENCES types (character), " .
            "integer integer NOT NULL DEFAULT 42, " .
            "decimal decimal(5,3) NOT NULL DEFAULT 1.2, " .
            "date date DEFAULT NULL);"
        );

        $processes[] = $this->createDbProcess(
            $dbConfig,
            "\COPY types_fk FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;"
        );
        foreach ($processes as $process) {
            $process->run();
            if (!$process->isSuccessful()) {
                $this->fail($process->getErrorOutput());
            }
        }
    }

    public function getConfig($driver = 'pgsql')
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'PgSQL';
        return $config;
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = new CsvFile($this->rootPath . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals($expectedCsvFile->getHeader(), $outputCsvFile->getHeader());
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertEquals($expectedArr[$i], $outputArr[$i]);
        }
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('pgsql'),
                'public' => $this->getEnv('pgsql', 'DB_SSH_KEY_PUBLIC', true)
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy'
        ];

        $app = new Application($config);
        $result = $app->run();

        $expectedCsvFile = new CsvFile($this->rootPath . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals($expectedCsvFile->getHeader(), $outputCsvFile->getHeader());
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertEquals($expectedArr[$i], $outputArr[$i]);
        }
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = new Application($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testInvalidCredentialsTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['user'] = "fakeguy";
        $app = new Application($config);

        try {
            $result = $app->run();
            $this->fail("Invalid credentials should throw exception");
        } catch (UserException $exception) {
            $this->assertStringStartsWith("Connection failed", $exception->getMessage());
        }
    }

    public function testInvalidCredentialsAppRun()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['#password'] = "fakepass";

        $app = new Application($config);
        try {
            $result = $app->run();
            $this->fail("Invalid credentials should throw exception");
        } catch (UserException $exception) {
            $this->assertStringStartsWith("Error connecting", $exception->getMessage());
        }
    }
    
    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config);

        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertCount(3, $result['tables']);

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
                                    'name' => 'col1',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 123,
                                    'nullable' => false,
                                    'default' => 'column 1',
                                    'ordinalPosition' => 1,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
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
                            0 =>
                                array (
                                    'name' => 'character',
                                    'type' => 'character varying',
                                    'primaryKey' => true,
                                    'length' => 123,
                                    'nullable' => false,
                                    'default' => '',
                                    'ordinalPosition' => 1,
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'type' => 'integer',
                                    'primaryKey' => false,
                                    'length' => 32,
                                    'nullable' => false,
                                    'default' => '42',
                                    'ordinalPosition' => 2,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'type' => 'numeric',
                                    'primaryKey' => false,
                                    'length' => '5,3',
                                    'nullable' => false,
                                    'default' => '1.2',
                                    'ordinalPosition' => 3,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
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
                                    'type' => 'character varying',
                                    'primaryKey' => false,
                                    'length' => 123,
                                    'nullable' => true,
                                    'default' => '',
                                    'ordinalPosition' => 1,
                                    'foreignKeyRefTable' => 'types',
                                    'foreignKeyRefColumn' => 'character',
                                    'foreignKeyRef' => 'types_fk_character_fkey',
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'type' => 'integer',
                                    'primaryKey' => false,
                                    'length' => 32,
                                    'nullable' => false,
                                    'default' => '42',
                                    'ordinalPosition' => 2,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'type' => 'numeric',
                                    'primaryKey' => false,
                                    'length' => '5,3',
                                    'nullable' => false,
                                    'default' => '1.2',
                                    'ordinalPosition' => 3,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'type' => 'date',
                                    'primaryKey' => false,
                                    'length' => null,
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 4,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testManifestMetadata()
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

        $app = new Application($config);

        $result = $app->run();

        $expectedTableMetadata[0] = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'types',
                ),
            1 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => 'public',
                ),
            2 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ),
        );
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
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    5 =>
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
                            'key' => 'KBC.datatype.length',
                            'value' => 32,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '42',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
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
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
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
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 4,
                        ),
                ),
        );

        $expectedTableMetadata[1] = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'types_fk',
                ),
            1 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => 'public',
                ),
            2 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'BASE TABLE',
                ),
        );
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
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 1,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'types',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.foreignKeyRefColumn',
                            'value' => 'character',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'types_fk_character_fkey',
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
                            'key' => 'KBC.datatype.length',
                            'value' => 32,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.datatype.default',
                            'value' => '42',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
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
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
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
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => 4,
                        ),
                ),
        );

        $outputManifests = [];
        foreach ($result['imported'] as $i => $filename) {
            $outputManifest = Yaml::parse(
                file_get_contents($this->dataDir . '/out/tables/' . $filename . '.csv.manifest')
            );
            $this->assertManifestMetadata($outputManifest, $expectedTableMetadata[$i], $expectedColumnMetadata[$i]);
        }
    }

    protected function assertManifestMetadata($outputManifest, $expectedTableMetadata, $expectedColumnMetadata)
    {
        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $this->assertArrayHasKey('column_metadata', $outputManifest);

        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testTableColumnsQuery()
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = new Application($config);

        $result = $app->run();

        $expectedCsvFile = new CsvFile($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals($expectedCsvFile->getHeader(), $outputCsvFile->getHeader());
        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertEquals($expectedArr[$i], $outputArr[$i]);
        }
    }
}
