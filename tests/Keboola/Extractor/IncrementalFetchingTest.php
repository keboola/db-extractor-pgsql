<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\Csv\CsvFile;

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
            new CsvFile(
                $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv'
            )
        );

        $previousValue = 0.0;
        foreach ($outputCsvFile as $key => $row) {
            $this->assertGreaterThan($previousValue, (float) $row[5]);
            $previousValue = (float) $row[5];
        }
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        try {
            $result = ($this->createApplication($config))->run();
            $this->fail('cannot use incremental fetching with advanced query, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
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
                "fakeCol",
                "Column [fakeCol] specified for incremental fetching was not found in the table",
            ],
            'column exists but is not auto-increment nor updating timestamp so should fail' => [
                "weird_name",
                "Column [weird_name] specified for incremental fetching is not a numeric or timestamp type column",
            ],
        ];
    }
}
