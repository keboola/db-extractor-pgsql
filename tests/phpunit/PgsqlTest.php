<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\PgsqlApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\PdoTestConnectionTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;
use PDO;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class PgsqlTest extends TestCase
{
    use PdoTestConnectionTrait;
    use ConfigTrait;
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;
    use TypesTableTrait;
    use EscapingTableTrait;

    protected string $dataDir = __DIR__ . '/data';

    protected PDO $connection;

    protected function setUp(): void
    {
        $this->connection = $this->createTestConnection();
        $this->removeAllTables();
        $this->closeSshTunnels();
        $fs = new Filesystem();
        if (!$fs->exists($this->dataDir)) {
            $fs->mkdir($this->dataDir . '/out/tables');
        }
        putenv('KBC_DATADIR=' . $this->dataDir);
    }

    public function testRunPDOEmptyTable(): void
    {
        $this->createTypesTable();
        $this->generateTypesRows();

        $config = $this->getRowConfig();
        $config['parameters']['forceFallback'] = true;
        $config['parameters']['table']['tableName'] = 'types';

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        $logger = new TestLogger();

        $app = new PgsqlApplication($logger);
        $app->execute();

        Assert::assertTrue($logger->hasInfo('Exported "4" rows to "in.c-main.types".'));
    }

    public function testManifestMetadata(): void
    {
        $this->createTypesTable();
        $this->generateTypesRows();
        $this->createTypesTable('types_fk', ['character' => 'varchar(123) REFERENCES types (character)']);
        $this->generateTypesRows('types_fk');

        $config = $this->getConfig();

        $config['parameters']['tables'][3] = $config['parameters']['tables'][0];
        $config['parameters']['tables'][3]['id'] = 4;
        $config['parameters']['tables'][3]['name'] = 'types_fk';
        $config['parameters']['tables'][3]['outputTable'] = 'in.c-main.types_fk';
        $config['parameters']['tables'][3]['primaryKey'] = null;
        $config['parameters']['tables'][3]['table']['tableName'] = 'types_fk';

        // use just 2 tables
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        $logger = new TestLogger();

        $app = new PgsqlApplication($logger);
        $app->execute();

        $expectedTableMetadata['in.c-main.types.csv.manifest'] = [
            [
                'key' => 'KBC.name',
                'value' => 'types',
            ],
            [
                'key' => 'KBC.sanitizedName',
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
        $expectedColumnMetadata['in.c-main.types.csv.manifest'] = [
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

        $expectedTableMetadata['in.c-main.types_fk.csv.manifest'] = [
            [
                'key' => 'KBC.name',
                'value' => 'types_fk',
            ],
            [
                'key' => 'KBC.sanitizedName',
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
        $expectedColumnMetadata['in.c-main.types_fk.csv.manifest'] = [
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

        $finder = new Finder();
        $manifestFiles = $finder->in($this->dataDir . '/out/tables/')->name('*.manifest')->files();

        foreach ($manifestFiles as $manifestFile) {
            $outputManifest = (array) json_decode(
                (string) file_get_contents($manifestFile->getPathname()),
                true,
            );
            $this->assertManifestMetadata(
                $outputManifest,
                $expectedTableMetadata[$manifestFile->getFilename()],
                $expectedColumnMetadata[$manifestFile->getFilename()],
            );
        }
    }

    protected function assertManifestMetadata(
        array $outputManifest,
        array $expectedTableMetadata,
        array $expectedColumnMetadata,
    ): void {
        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        $this->assertArrayHasKey('column_metadata', $outputManifest);

        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testEmptyPrimaryKeyString(): void
    {
        $this->createTypesTable();
        $this->generateTypesRows();

        $config = $this->getRowConfig();

        $config['parameters']['primaryKey'] = [''];

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        $logger = new TestLogger();

        $app = new PgsqlApplication($logger);
        $app->execute();

        Assert::assertTrue($logger->hasInfo('Exported "4" rows to "in.c-main.types".'));
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
