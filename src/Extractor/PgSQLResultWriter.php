<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Iterator;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\Exception\NoRowsException;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class PgSQLResultWriter extends DefaultResultWriter
{

    protected CsvWriter $csvWriter;

    public function resetSettings(string $csvFilePath): void
    {
        $this->rowsCount = 0;
        $this->lastRow = null;

        // Create CSV writer
        $this->csvWriter = $this->createCsvWriter($csvFilePath);
    }

    public function writeToCsv(
        QueryResult $result,
        ExportConfig $exportConfig,
        string $csvFilePath,
        bool $resetSetting = false,
        ?callable $rowCallback = null
    ): ExportResult {
        if ($resetSetting) {
            $this->resetSettings($csvFilePath);
        }

        // Get iterator
        $iterator = $this->getIterator($result);

        // Write rows
        try {
            $this->writeRows($exportConfig, $iterator, $this->csvWriter, $rowCallback);
        } catch (NoRowsException $e) {
            @unlink($csvFilePath); // no rows, no file
            return new ExportResult($csvFilePath, 0, $this->getMaxValueFromState($exportConfig));
        } finally {
            $result->closeCursor();
        }

        $incFetchingColMaxValue = $this->getIncrementalFetchingMaxValue($exportConfig, $this->lastRow);
        return new ExportResult($csvFilePath, $this->rowsCount, $incFetchingColMaxValue);
    }

    protected function writeRows(
        ExportConfig $exportConfig,
        Iterator $iterator,
        CsvWriter $csvWriter,
        ?callable $rowCallback = null
    ): void {
        if ($this->rowsCount === 0) {
            // No rows found ?
            if (!$iterator->valid()) {
                throw new NoRowsException();
            }

            // With custom query are no metadata in manifest, so header must be present
            $includeHeader = $exportConfig->hasQuery();

            // Rows found, iterate!
            $resultRow = $iterator->current();
            $iterator->next();

            // Write header and first line
            if ($includeHeader) {
                $this->writeHeader(array_keys($resultRow), $csvWriter);
            }

            if ($rowCallback) {
                $resultRow = $rowCallback($resultRow);
            }

            $this->writeRow($resultRow, $csvWriter);
            $this->rowsCount = 1;

            // Write the rest
            $this->lastRow = $resultRow;
        }

        while ($iterator->valid()) {
            $resultRow = $iterator->current();
            if ($rowCallback) {
                $resultRow = $rowCallback($resultRow);
            }
            $this->writeRow($resultRow, $csvWriter);
            $iterator->next();

            $this->lastRow = $resultRow;
            $this->rowsCount++;
        }
    }
}
