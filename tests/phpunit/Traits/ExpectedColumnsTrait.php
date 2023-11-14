<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

use Keboola\DbExtractor\Exception\UserException;

trait ExpectedColumnsTrait
{
    public function expectedTableColumns(string $schema, string $table): array
    {
        if ($schema === 'temp_schema') {
            if ($table === 'ext_sales') {
                return [
                    0 =>
                        [
                            'name' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    1 =>
                        [
                            'name' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    2 =>
                        [
                            'name' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    3 =>
                        [
                            'name' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    4 =>
                        [
                            'name' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    5 =>
                        [
                            'name' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    6 =>
                        [
                            'name' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    7 =>
                        [
                            'name' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    8 =>
                        [
                            'name' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    9 =>
                        [
                            'name' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    10 =>
                        [
                            'name' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    11 =>
                        [
                            'name' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                ];
            } else {
                throw new UserException(sprintf('Unexpected test table %s in schema %s', $table, $schema));
            }
        } elseif ($schema === 'test') {
            switch ($table) {
                case 'sales':
                    return [
                        [
                            'name' => 'usergender',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'usercity',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'usersentiment',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'zipcode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'sku',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'createdat',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'category',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'price',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'county',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'countycode',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'userstate',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'categorygroup',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'escaping':
                    return [
                        [
                            'name' => 'col1',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'col2',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'emoji':
                    return [
                        [
                            'name' => 'emoji',
                            'type' => 'text',
                            'primaryKey' => false,
                        ],
                    ];
                case 'auto_increment_timestamp':
                    return [
                        [
                            'name' => '_weird-I-d',
                            'type' => 'int',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'weird-Name',
                            'type' => 'varchar',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'timestamp',
                            'type' => 'timestamp',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'datetime',
                            'type' => 'datetime',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'intColumn',
                            'type' => 'int',
                            'primaryKey' => false,
                        ],
                        [
                            'name' => 'decimalColumn',
                            'type' => 'decimal',
                            'primaryKey' => false,
                        ],
                    ];
            }
        }

        throw new UserException(sprintf('Unexpected schema %s', $schema));
    }
}
