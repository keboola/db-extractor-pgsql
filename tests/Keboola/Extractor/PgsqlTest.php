<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;

class PgsqlTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        define('APP_NAME', 'ex-db-pgsql');
        $this->app = new Application($this->getConfig('pgsql'));
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = ROOT_PATH . 'vendor/keboola/db-extractor-common/tests/data/escaping.csv';
//        $expectedManifestFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('ok', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
//        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

}
