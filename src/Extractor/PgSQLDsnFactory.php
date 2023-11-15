<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\Temp\Temp;

class PgSQLDsnFactory
{
    public static function createForPdo(DatabaseConfig $dbConfig): string
    {
        $dsn = self::create($dbConfig);

        // User name is PDO parameter, not part of the DSN
        unset($dsn['user']);

        // Convert to: k1=v1;k2=v2
        // PDO needs values separated by ;
        $parts = [];
        foreach ($dsn as $k => $v) {
            $parts[] = $k . '='. $v;
        }
        return implode(';', $parts) . ';';
    }

    public static function createForCli(DatabaseConfig $dbConfig): string
    {
        $dsn = self::create($dbConfig);

        // Convert to: k1=v1 k2=v2
        // Psql CLI tool needs values separated by space
        $parts = [];
        foreach ($dsn as $k => $v) {
            $parts[] = $k . '='. $v;
        }
        return implode(' ', $parts);
    }

    private static function create(DatabaseConfig $dbConfig): array
    {
        $dsn = [];
        $port = $dbConfig->hasPort() ? $dbConfig->getPort() : '5432';
        $dsn['host'] = $dbConfig->getHost();
        $dsn['port'] = $port;
        $dsn['user'] = $dbConfig->getUsername();
        $dsn['dbname'] = $dbConfig->getDatabase();

        if ($dbConfig->hasSSLConnection()) {
            $tempDir = new Temp('ssl');
            $sslConnection = $dbConfig->getSslConnectionConfig();

            if ($sslConnection->isIgnoreCertificateCn()) {
                $dsn['sslmode'] = 'require';
            } elseif ($sslConnection->isVerifyServerCert()) {
                $dsn['sslmode'] = 'verify-full';
            } else {
                $dsn['sslmode'] = 'verify-ca';
            }

            if ($sslConnection->hasCa()) {
                $dsn['sslrootcert'] = SslHelper::createSSLFile($tempDir, $sslConnection->getCa());
            }

            if ($sslConnection->hasCert()) {
                $dsn['sslcert'] = SslHelper::createSSLFile($tempDir, $sslConnection->getCert());
            }

            if ($sslConnection->hasKey()) {
                $keyFile = SslHelper::createSSLFile($tempDir, $sslConnection->getKey());
                chmod($keyFile, 0600);
                $dsn['sslkey'] = $keyFile;
            }
        }

        return $dsn;
    }
}
