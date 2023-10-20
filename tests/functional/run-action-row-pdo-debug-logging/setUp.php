<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    putenv('KBC_COMPONENT_RUN_MODE=debug');
    $manager = new DatabaseManager($test->getConnection());

    // escaping table
    $manager->createEscapingTable();
    $manager->generateEscapingRows();
};
