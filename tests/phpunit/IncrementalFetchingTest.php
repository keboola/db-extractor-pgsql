<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\Component\Logger;
use Psr\Log\Test\TestLogger;

class IncrementalFetchingTest extends BaseTest
{
    protected function createAutoIncrementAndTimestampTable(): void
    {
        $incrTableProcesses = [];
        $incrTableProcesses[] = $this->createDbProcess('CREATE SEQUENCE user_id_seq;');
        $incrTableProcesses[] = $this->createDbProcess('CREATE TABLE auto_increment_timestamp (
            "_weird_id" INT NOT NULL DEFAULT nextval(\'user_id_seq\'),
            "serial_id" SERIAL,
            "weird_name" character varying (30) NOT NULL DEFAULT \'pam\',
            "timestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "floatcolumn" float DEFAULT 1.23,
            "decimalcolumn" DECIMAL(10,2) DEFAULT 10.2,
            PRIMARY KEY ("_weird_id")  
        )');
        $incrTableProcesses[] = $this->createDbProcess(
            'INSERT INTO auto_increment_timestamp ' .
            '(weird_Name, floatcolumn, decimalcolumn) VALUES (\'george\', 2.2, 20.2)'
        );

        $this->runProcesses($incrTableProcesses);
        // Stagger the new column input timestamps
        sleep(1);

        $incrTableProcesses = [
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp ' .
                '(weird_Name, floatcolumn, decimalcolumn) VALUES (\'henry\', 3.3, 30.3)'
            ),
        ];
        $this->runProcesses($incrTableProcesses);
    }

    private function createGeoHellTable(): void
    {
        $incrTableProcesses = [];
        $incrTableProcesses[] = $this->createDbProcess('CREATE TABLE moving_targets (
            "int_id" INT NOT NULL,
            "name" character varying (30) DEFAULT NULL,
            "another_string" character varying (30) DEFAULT NULL,
            "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "someint" INT DEFAULT NULL,
            PRIMARY KEY ("int_id")  
        )');

        $incrTableProcesses[] = $this->createDbProcess('ALTER TABLE moving_targets DROP COLUMN another_string;');

        $incrTableProcesses[] = $this->createDbProcess(
            'INSERT INTO moving_targets' .
            '(int_id, name, someint) VALUES ' .
            '(2, \'george\', 42),' .
            '(3, \'ed\', 43)'
        );

        $this->runProcesses($incrTableProcesses);
        // Stagger the new column input timestamps
        sleep(1);

        $incrTableProcesses = [
            $this->createDbProcess(
                'INSERT INTO moving_targets ' .
                '(int_id, name, someint) VALUES ' .
                '(4, \'fred\', 55),' .
                '(5, \'art\', 57)'
            ),
        ];
        $this->runProcesses($incrTableProcesses);
    }

    protected function getIncrementalFetchingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'public',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_weird_id'];
        $config['parameters']['incrementalFetchingColumn'] = '_weird_id';
        return $config;
    }

    public function testBrokenIncrFetchDroppedColumn(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['table'] = [
            'tableName' => 'moving_targets',
            'schema' => 'public',
        ];
        $config['parameters']['name'] = 'moving_targets';
        $config['parameters']['outputTable'] = 'in.c-main.moving_targets';
        $config['parameters']['primaryKey'] = ['int_id'];
        $config['parameters']['incrementalFetchingColumn'] = 'updated_at';
        $this->createGeoHellTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.moving_targets',
                'rows' => 4,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);
        // The dropped column will make the incr fetch retrieve the wrong value
        // because it is based on the columns ordinal position which does not change when a column is dropped
        $this->assertNotEquals(57, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should only have the last 2 rows since they have the same last updqted time
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(2, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO moving_targets (int_id, name, someint) VALUES (12, \'charles\', 77), (32, \'william\', 78)'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(4, $newResult['imported']['rows']);
        $this->assertNotEquals(78, $result['state']['lastFetchedRow']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp (weird_name) VALUES (\'charles\'), (\'william\')'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = '_weird_id';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp (weird_Name) VALUES (\'charles\'), (\'william\')'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingBySerial(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'serial_id';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp ' .
                '(weird_name, floatcolumn) VALUES (\'charles\', 4.4), (\'william\', 7.7)'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByFloat(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'floatcolumn';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(3.3, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp ' .
                '(weird_name, floatcolumn) VALUES (\'charles\', 4.4), (\'william\', 7.7)'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(7.7, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'decimalcolumn';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(30.3, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.  Only the one row that has decimal >= to 30.3 should be included
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp ' .
                '(weird_name, decimalColumn) VALUES (\'charles\', 4.4), (\'william\', 70.7)'
            ),
        ]);

        $newResult = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(70.7, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        sleep(2);
        // for the next fetch should contain the second row the limit must be 2 since we are using >=
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $result = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    public function testIncrementalOrdering(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'decimalcolumn';

        //check that output state contains expected information
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp ' .
                '(weird_name, decimalColumn) VALUES (\'william\', 70.7), (\'charles\', 4.4)'
            ),
        ]);

        sleep(2);
        // the next fetch should be empty
        $result = ($this->createApplication($config))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(70.7, $result['state']['lastFetchedRow']);
        $this->assertEquals(4, $result['imported']['rows']);

        $outputCsvFile = iterator_to_array(
            new CsvReader(
                $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv'
            )
        );

        $previousValue = 0.0;
        foreach ($outputCsvFile as $key => $row) {
            $this->assertGreaterThan($previousValue, (float) $row[5]);
            $previousValue = (float) $row[5];
        }
    }

    public function testIncrementalFetchingPDOFallback(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();

        $result = ($this->createApplication($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->runProcesses([
            $this->createDbProcess(
                'INSERT INTO auto_increment_timestamp (weird_Name) VALUES (\'charles\'), (\'william\')'
            ),
        ]);
        // set the config to use the pdo fallback
        $config['parameters']['forceFallback'] = true;
        // use a test logger to make sure we can tell that it actually falls back to pdo
        $logger = new TestLogger();
        $newResult = ($this->createApplication($config, $result['state'], $logger))->run();

        $this->assertTrue($logger->hasWarningThatContains('Forcing extractor to use PDO fallback fetching'));
        $this->assertTrue($logger->hasInfoThatContains(
            "Executing query 'auto-increment-timestamp' via PDO ..."
        ));
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage(
            'The "incrementalFetchingColumn" is configured, but incremental fetching is not supported for custom query.'
        );
        $app = $this->createApplication($config);
        $app->run();
    }

    /**
     * @dataProvider invalidColumnProvider
     */
    public function testIncrementalFetchingInvalidColumns(string $column, string $expectedExceptionMessage): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = $column;

        $this->expectException(UserException::class);
        $this->expectExceptionMessage($expectedExceptionMessage);
        $this->createApplication($config)->run();
    }

    public function invalidColumnProvider(): array
    {
        return [
            'column does not exist' => [
                'fakeCol',
                'Column "fakeCol" specified for incremental fetching was not found in the table',
            ],
            'column exists but is not auto-increment nor updating timestamp so should fail' => [
                'weird_name',
                'Column "weird_name" specified for incremental fetching has unexpected type "STRING", ' .
                'expected: "INTEGER", "NUMERIC", "FLOAT", "TIMESTAMP".',
            ],
        ];
    }

    public function testIncrementalFetchingColumnNull(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = null;

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
    }

    public function testIncrementalFetchingColumnLimit(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $config['parameters']['incrementalFetchingLimit'] = null;

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
    }
}
