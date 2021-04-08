<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Configuration\PgsqlExportConfig;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class PgSQLResultWriter extends DefaultResultWriter
{
    private bool $replaceBooleans;

    public function writeToCsv(QueryResult $result, ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!$exportConfig instanceof PgsqlExportConfig) {
            throw new InvalidArgumentException('PgsqlExportConfig expected.');
        }

        $this->replaceBooleans = $exportConfig->getReplaceBooleans();
        return parent::writeToCsv($result, $exportConfig, $csvFilePath);
    }

    protected function writeRow(array $row, CsvWriter $csvWriter): void
    {
        if ($this->replaceBooleans) {
            array_walk($row, function (&$item): void {
                if (is_bool($item)) {
                    $item = $item === true ? 't' : 'f';
                }
            });
        }

        parent::writeRow($row, $csvWriter);
    }
}
