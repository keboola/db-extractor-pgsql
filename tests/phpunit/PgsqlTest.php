<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Exception\UserException;

class PgsqlTest extends BaseTest
{
    public function testRunConfig(): void
    {
        $config = $this->getConfig('pgsql');
        $result = $this->createApplication($config)->run();
        $expectedCsvFile = new CsvReader($this->dataDir . '/pgsql/escaping.csv');
        $outputCsvFile = new CsvReader(
            $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv'
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputManifestFile);
        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);

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
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
        ];

        $app = $this->createApplication($config);
        $result = $app->run();

        $expectedCsvFile = new CsvReader($this->dataDir . '/pgsql/escaping.csv');
        $outputCsvFile = new CsvReader(
            $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv'
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $this->assertFileExists($outputManifestFile);

        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);
        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['columns']);
        $this->assertEquals(['funnycol', 'sadcol'], $outputManifest['primary_key']);

        $this->assertEquals('success', $result['status']);

        $outputArr = iterator_to_array($outputCsvFile);
        $expectedArr = iterator_to_array($expectedCsvFile);
        for ($i = 1; $i < count($expectedArr); $i++) {
            $this->assertContains($expectedArr[$i], $outputArr);
        }
    }

    public function testRunPDOEmptyTable(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['forceFallback'] = true;
        $config['parameters']['table']['tableName'] = 'empty_table';

        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testInvalidCredentialsTestConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';

        $config['parameters']['db']['user'] = 'fakeguy';
        $app = $this->createApplication($config);

        try {
            $app->run();
            $this->fail('Invalid credentials should throw exception');
        } catch (UserException $exception) {
            $this->assertStringStartsWith('Connection failed', $exception->getMessage());
        }
    }

    public function testInvalidCredentialsAppRun(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['#password'] = 'fakepass';

        $app = $this->createApplication($config);
        try {
            $app->run();
            $this->fail('Invalid credentials should throw exception');
        } catch (UserException $exception) {
            $this->assertStringStartsWith('Error connecting', $exception->getMessage());
        }
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        unset($config['parameters']['tables']);
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertCount(5, $result['tables']);

        $expectedData = [
            [
                'name' => 'empty_table',
                'schema' => 'public',
                'type' => 'table',
                'columns' => [
                    [
                        'name' => 'integer',
                        'sanitizedName' => 'integer',
                        'type' => 'integer',
                        'primaryKey' => false,
                        'nullable' => false,
                        'default' => '42',
                        'ordinalPosition' => 1,
                        'uniqueKey' => false,
                    ],
                    [
                        'name' => 'date',
                        'sanitizedName' => 'date',
                        'type' => 'date',
                        'primaryKey' => false,
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'uniqueKey' => false,
                    ],
                ],
            ],
            [
                'name' => 'escaping',
                'schema' => 'public',
                'type' => 'table',
                'columns' =>
                    [
                        [
                            'name' => '_funnycol',
                            'sanitizedName' => 'funnycol',
                            'type' => 'character varying',
                            'primaryKey' => true,
                            'length' => '123',
                            'nullable' => false,
                            'default' => 'column 1',
                            'ordinalPosition' => 1,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => '_sadcol',
                            'sanitizedName' => 'sadcol',
                            'type' => 'character varying',
                            'primaryKey' => true,
                            'length' => '221',
                            'nullable' => false,
                            'default' => 'column 2',
                            'ordinalPosition' => 2,
                            'uniqueKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'types',
                'schema' => 'public',
                'type' => 'table',
                'columns' =>
                    [
                        [
                            'name' => 'character',
                            'sanitizedName' => 'character',
                            'type' => 'character varying',
                            'primaryKey' => true,
                            'length' => 123,
                            'nullable' => false,
                            'ordinalPosition' => 1,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'integer',
                            'sanitizedName' => 'integer',
                            'type' => 'integer',
                            'primaryKey' => false,
                            'nullable' => false,
                            'default' => '42',
                            'ordinalPosition' => 2,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'decimal',
                            'sanitizedName' => 'decimal',
                            'type' => 'numeric',
                            'primaryKey' => false,
                            'length' => '5,3',
                            'nullable' => false,
                            'default' => '1.2',
                            'ordinalPosition' => 3,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'boolean',
                            'sanitizedName' => 'boolean',
                            'type' => 'boolean',
                            'primaryKey' => false,
                            'nullable' => false,
                            'ordinalPosition' => 4,
                            'uniqueKey' => false,
                            'default' => 'false',
                        ],
                        [
                            'name' => 'date',
                            'sanitizedName' => 'date',
                            'type' => 'date',
                            'primaryKey' => false,
                            'nullable' => true,
                            'ordinalPosition' => 5,
                            'uniqueKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'types_fk',
                'schema' => 'public',
                'type' => 'table',
                'columns' =>
                    [
                        [
                            'name' => 'character',
                            'sanitizedName' => 'character',
                            'type' => 'character varying',
                            'primaryKey' => false,
                            'length' => '123',
                            'nullable' => true,
                            'ordinalPosition' => 1,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'integer',
                            'sanitizedName' => 'integer',
                            'type' => 'integer',
                            'primaryKey' => false,
                            'nullable' => false,
                            'default' => '42',
                            'ordinalPosition' => 2,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'decimal',
                            'sanitizedName' => 'decimal',
                            'type' => 'numeric',
                            'primaryKey' => false,
                            'length' => '5,3',
                            'nullable' => false,
                            'default' => '1.2',
                            'ordinalPosition' => 3,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => 'boolean',
                            'sanitizedName' => 'boolean',
                            'type' => 'boolean',
                            'primaryKey' => false,
                            'nullable' => false,
                            'ordinalPosition' => 4,
                            'uniqueKey' => false,
                            'default' => 'false',
                        ],
                        [
                            'name' => 'date',
                            'sanitizedName' => 'date',
                            'type' => 'date',
                            'primaryKey' => false,
                            'nullable' => true,
                            'ordinalPosition' => 5,
                            'uniqueKey' => false,
                        ],
                    ],
            ],
            [
                'name' => 'escaping',
                'schema' => 'testing',
                'type' => 'table',
                'columns' =>
                    [
                        [
                            'name' => '_funnycol',
                            'sanitizedName' => 'funnycol',
                            'type' => 'character varying',
                            'primaryKey' => true,
                            'length' => '123',
                            'nullable' => false,
                            'default' => 'column 1',
                            'ordinalPosition' => 1,
                            'uniqueKey' => false,
                        ],
                        [
                            'name' => '_sadcol',
                            'sanitizedName' => 'sadcol',
                            'type' => 'character varying',
                            'primaryKey' => true,
                            'length' => '221',
                            'nullable' => false,
                            'default' => 'column 2',
                            'ordinalPosition' => 2,
                            'uniqueKey' => false,
                        ],
                    ],
            ],
        ];

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

        // use just 2 tables
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $expectedTableMetadata[0] = [
            [
                'key' => 'KBC.name',
                'value' => 'types',
            ],
            [
                'key' => 'KBC.schema',
                'value' => 'public',
            ],
            [
                'key' => 'KBC.type',
                'value' => 'table',
            ],
        ];
        $expectedColumnMetadata[0] = [
            'character' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'character varying',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => 123,
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'character',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'character',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 1,
                    ],
                ],
            'integer' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => '42',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 2,
                    ],
                ],
            'decimal' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'numeric',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'NUMERIC',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '5,3',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => '1.2',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'decimal',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'decimal',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 3,
                    ],
                ],
            'date' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'DATE',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 5,
                    ],
                ],
            'boolean' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'BOOLEAN',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => 'false',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 4,
                ],
            ],
        ];

        $expectedTableMetadata[1] = [
            [
                'key' => 'KBC.name',
                'value' => 'types_fk',
            ],
            [
                'key' => 'KBC.schema',
                'value' => 'public',
            ],
            [
                'key' => 'KBC.type',
                'value' => 'table',
            ],
        ];
        $expectedColumnMetadata[1] = [
            'character' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'character varying',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => 123,
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'character',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'character',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 1,
                    ],
                ],
            'integer' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => '42',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'integer',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 2,
                    ],
                ],
            'decimal' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'numeric',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'NUMERIC',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '5,3',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => '1.2',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'decimal',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'decimal',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 3,
                    ],
                ],
            'date' =>
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => true,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'DATE',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'date',
                    ],
                    [
                        'key' => 'KBC.primaryKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => 5,
                    ],
                ],

            'boolean' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'BOOLEAN',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => 'false',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'boolean',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 4,
                ],
            ],
        ];

        foreach ($result['imported'] as $i => $outputArray) {
            $filenameManifest = $this->dataDir . '/out/tables/' . $outputArray['outputTable'] . '.csv.manifest';
            $outputManifest = json_decode(
                (string) file_get_contents($filenameManifest),
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

        $app = $this->createApplication($config);

        $result = $app->run();

        $expectedCsvFile = new CsvReader($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvReader(
            $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv'
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputManifestFile);

        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);
        $this->assertEquals(['character', 'integer', 'decimal', 'boolean', 'date'], $outputManifest['columns']);
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

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);

        $expectedCsvFile = new CsvReader($this->dataDir . '/pgsql/types.csv');
        $outputCsvFile = new CsvReader(
            $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv'
        );
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);
        // check that the manifest has the correct column ordering
        $this->assertEquals(['character', 'integer', 'decimal', 'boolean', 'date'], $outputManifest['columns']);

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
        // $this->markTestSkipped('No need to run this test every time.');
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
        $columnsSql = '';
        for ($columnCount = 0; $columnCount < $numberOfColumnsPerTable; $columnCount++) {
            $columnsSql .= sprintf(', "col_%d" VARCHAR(50) NOT NULL DEFAULT \'\'', $columnCount);
        }

        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            $processes[] = $this->createDbProcess(sprintf('CREATE SCHEMA testschema_%d', $schemaCount));
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $processes[] = $this->createDbProcess(
                    sprintf(
                        'CREATE TABLE testschema_%d.testtable_%d (ID SERIAL%s, PRIMARY KEY (ID))',
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
        unset($config['parameters']['tables']);
        $config['action'] = 'getTables';

        $jobStartTime = time();
        $result = $this->createApplication($config)->run();
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

    public function testBadQuery(): void
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]['columns']);
        unset($config['parameters']['tables'][2]['table']);
        $config['parameters']['tables'][2]['query'] = 'SELECT %%% FROM types';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Error executing [in.c-main.types]: SQLSTATE[42601]:');

        $app = $this->createApplication($config);
        $app->run();
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
