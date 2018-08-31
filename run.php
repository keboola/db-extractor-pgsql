<?php

declare(strict_types=1);

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/vendor/autoload.php");

$logger = new Logger('ex-db-pgsql');

$runAction = true;

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $jsonDecode = new JsonDecode(true);

    if (file_exists($arguments["data"] . "/config.yml")) {
        $config = Yaml::parse(
            file_get_contents($arguments["data"] . "/config.yml")
        );
    } else if (file_exists($arguments["data"] . "/config.json")) {
        $config = $jsonDecode->decode(
            file_get_contents($arguments["data"] . '/config.json'),
            JsonEncoder::FORMAT
        );
    } else {
        throw new UserException('Configuration file not found.');
    }

    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'PgSQL';

    $app = new Application($config, $logger);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();
    if (!$runAction) {
        echo json_encode($app->run());
    }
    $app['logger']->log('info', "Extractor finished successfully.");
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage());
    if (!$runAction) {
        echo $e->getMessage();
    }
    exit(1);
} catch (Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
