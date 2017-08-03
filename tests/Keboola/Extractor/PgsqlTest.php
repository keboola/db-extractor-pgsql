<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class PgsqlTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-pgsql');
        }
        $config = $this->getConfig();
        $this->app = new Application($config);

        $dbConfig = $config['parameters']['db'];

        // create test tables
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"DROP TABLE IF EXISTS escaping;\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"CREATE TABLE escaping (col1 varchar(123) NOT NULL DEFAULT 'column 1', col2 varchar(221) NOT NULL DEFAULT 'column 2', PRIMARY KEY (col1, col2));\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"\COPY escaping FROM 'vendor/keboola/db-extractor-common/tests/data/escaping.csv' WITH DELIMITER ',' CSV HEADER;\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }

        // create test tables
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"DROP TABLE IF EXISTS types;\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"CREATE TABLE types " .
                    "(character varchar(123) NOT NULL DEFAULT 'default string', " .
                    "integer integer NOT NULL DEFAULT 42, " .
                    "decimal decimal(5,3) NOT NULL DEFAULT 1.2, " .
                    "date date DEFAULT NULL);\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
        }
        $process = new Process(sprintf(
            "PGPASSWORD='%s' psql -h %s -p %s -U %s -d %s -w -c \"\COPY types FROM 'tests/data/pgsql/types.csv' WITH DELIMITER ',' CSV HEADER;\"",
            $dbConfig['password'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['user'],
            $dbConfig['database']
        ));
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getErrorOutput());
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
        $expectedCsvFile = new CsvFile(ROOT_PATH . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
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
                '#private' => $this->getEnv('pgsql', 'DB_SSH_KEY_PRIVATE', true),
                'public' => $this->getEnv('pgsql', 'DB_SSH_KEY_PUBLIC', true)
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy'
        ];

        $app = new Application($config);
        $result = $app->run();

        $expectedCsvFile = new CsvFile(ROOT_PATH . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv');
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
        $this->assertCount(2, $result['tables']);
        foreach ($result['tables'] as $table) {
            $this->assertArrayHasKey('name', $table);
            $this->assertArrayHasKey('columns', $table);
            switch ($table['name']) {
                case 'escaping':
                    $this->assertCount(2, $table['columns']);
                    break;
                case 'types':
                    $this->assertCount(4, $table['columns']);
                    break;
                default:
                    $this->fail("Unexpected table found: " . $table['name']);
            }
            foreach ($table['columns'] as $i => $column) {
                // keys
                $this->assertArrayHasKey('name', $column);
                $this->assertArrayHasKey('type', $column);
                $this->assertArrayHasKey('length', $column);
                $this->assertArrayHasKey('default', $column);
                $this->assertArrayHasKey('nullable', $column);
                $this->assertArrayHasKey('primaryKey', $column);
                $this->assertArrayHasKey('ordinalPosition', $column);

                $this->assertEquals($i + 1, $column['ordinalPosition']);
                switch ($column['name']) {
                    case 'col1':
                        $this->assertEquals("character varying", $column['type']);
                        $this->assertEquals(123, $column['length']);
                        $this->assertFalse($column['nullable']);
                        $this->assertEquals('column 1', $column['default']);
                        $this->asserttrue($column['primaryKey']);
                        break;
                    case 'col2':
                        $this->assertEquals("character varying", $column['type']);
                        $this->assertEquals(221, $column['length']);
                        $this->assertFalse($column['nullable']);
                        $this->assertEquals('column 2', $column['default']);
                        $this->asserttrue($column['primaryKey']);
                        break;
                    case 'character':
                        $this->assertEquals("character varying", $column['type']);
                        $this->assertEquals(123, $column['length']);
                        $this->assertFalse($column['nullable']);
                        $this->assertEquals('default string', $column['default']);
                        $this->assertFalse($column['primaryKey']);
                        break;
                    case 'integer':
                        $this->assertEquals("integer", $column['type']);
                        $this->assertEquals(32, $column['length']);
                        $this->assertFalse($column['nullable']);
                        $this->assertEquals(42, $column['default']);
                        $this->assertFalse($column['primaryKey']);
                        break;
                    case 'decimal':
                        $this->assertEquals("numeric", $column['type']);
                        $this->assertEquals("5,3", $column['length']);
                        $this->assertFalse($column['nullable']);
                        $this->assertEquals(1.2, $column['default']);
                        $this->assertFalse($column['primaryKey']);
                        break;
                    case 'date':
                        $this->assertEquals("date", $column['type']);
                        $this->assertNull($column['length']);
                        $this->assertTrue($column['nullable']);
                        $this->assertNull($column['default']);
                        $this->assertFalse($column['primaryKey']);
                        break;
                    default:
                        $this->fail("Unexpected column found: " . $column['name']);
                        break;
                }
            }
        }
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        $config['parameters']['tables'][0]['columns'] = ["character","integer","decimal","date"];
        $config['parameters']['tables'][0]['table'] = 'types';
        $config['parameters']['tables'][0]['query'] = "SELECT \"character\",\"integer\",\"decimal\",\"date\" FROM types";
        // use just 1 table
        unset($config['parameters']['tables'][1]);

        $app = new Application($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.name':
                    $this->assertEquals('types', $metadata['value']);
                    break;
                case 'KBC.schema':
                    $this->assertEquals('public', $metadata['value']);
                    break;
                case 'KBC.type':
                    $this->assertEquals('BASE TABLE', $metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);
        $colNum = 0;
        foreach ($outputManifest['column_metadata'] as $column => $metadataList) {
            $colNum++;
            foreach ($metadataList as $metadata) {
                $this->assertArrayHasKey('key', $metadata);
                $this->assertArrayHasKey('value', $metadata);
                switch ($metadata['key']) {
                    case 'KBC.datatype.type':
                        $this->assertContains($metadata['value'], ['character varying', 'integer', 'numeric', 'date']);
                        break;
                    case 'KBC.datatype.basetype':
                        $this->assertContains($metadata['value'], ['STRING', 'INTEGER', 'NUMERIC', 'DATE']);
                        break;
                    case 'KBC.datatype.nullable':
                        if ($column === 'date') {
                            $this->assertTrue($metadata['value']);
                        } else {
                            $this->assertFalse($metadata['value']);
                        }
                        break;
                    case 'KBC.datatype.default':
                        if ($column === 'date') {
                            $this->assertNull($metadata['value']);
                        } else {
                            $this->assertNotNull($metadata['value']);
                        }
                        break;
                    case 'KBC.datatype.length':
                        if ($column === 'date') {
                            $this->assertNull($metadata['value']);
                        } else {
                            $this->assertNotNull($metadata['value']);
                        }
                        break;
                    case 'KBC.primaryKey':
                        $this->assertFalse($metadata['value']);
                        break;
                    case 'KBC.ordinalPosition':
                        $this->assertEquals($colNum, $metadata['value']);
                        break;
                    default:
                        $this->fail("Unnexpected metadata key " . $metadata['key']);
                }
            }
        }
    }
}
