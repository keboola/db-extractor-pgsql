<?php

declare(strict_types=1);

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/vendor/autoload.php");

$logger = new Logger('ex-db-pgsql');

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }
    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'PgSQL';

    $app = new Application($config, $logger);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    echo json_encode($app->run());
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), $e->getData());
    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Throwable $e) {
    $logger->log(
        'error',
        $e->getMessage(),
        [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace(),
        ]
    );
    exit(2);
}
exit(0);
