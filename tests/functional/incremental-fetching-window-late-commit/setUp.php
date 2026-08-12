<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // A small table with DISTINCT timestamps (no ties) so row order in the output is fully
    // deterministic across the whole Postgres version test matrix.
    $manager->createTable('incremental_window_late_commit', [
        'id' => 'INTEGER NOT NULL PRIMARY KEY',
        'name' => 'VARCHAR(50) NOT NULL',
        'ts' => 'TIMESTAMP NOT NULL',
    ]);

    $manager->insertRows(
        'incremental_window_late_commit',
        ['id', 'name', 'ts'],
        [
            [1, 'row-1', '2021-01-05 10:00:00'],
            [2, 'row-2', '2021-01-05 10:05:00'],
            // Late commit: this row's "ts" (09:55:00) is BEFORE the watermark stored in
            // source/data/in/state.json (10:00:00). Under the old "col >= lastFetchedRow"
            // behaviour it would be permanently skipped on this run. With an incremental
            // fetching WINDOW start configured, the watermark is ignored and the configured
            // range is re-scanned, so this row must appear in the output.
            [3, 'late-row', '2021-01-05 09:55:00'],
            [4, 'row-4', '2021-01-05 10:10:00'],
        ],
    );
};
