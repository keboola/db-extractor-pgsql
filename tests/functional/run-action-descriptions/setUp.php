<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // Table carrying COMMENT ON TABLE / COMMENT ON COLUMN
    $manager->createCommentsTable();
    $manager->addCommentsToTable();
    $manager->generateCommentsRows();
};
