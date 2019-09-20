<?php

declare(strict_types=1);

use \Keboola\DbExtractor\PgsqlApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

require_once(__DIR__ . '/vendor/autoload.php');

$logger = new Logger('ex-db-pgsql');

$runAction = true;

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }

    $jsonDecode = new JsonDecode(true);

    if (file_exists($arguments['data'] . '/config.yml')) {
        $config = Yaml::parse(
            file_get_contents($arguments['data'] . '/config.yml')
        );
    } else if (file_exists($arguments['data'] . '/config.json')) {
        $config = $jsonDecode->decode(
            file_get_contents($arguments['data'] . '/config.json'),
            JsonEncoder::FORMAT
        );
    } else {
        throw new UserException('Configuration file not found.');
    }

    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'PgSQL';

    // get the state
    $inputState = [];
    $inputStateFile = $arguments['data'] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = $jsonDecode->decode(
            file_get_contents($inputStateFile),
            JsonEncoder::FORMAT
        );
    }

    $app = new PgsqlApplication($config, $logger, $inputState, $arguments['data']);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();
    if (!$runAction) {
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $arguments['data'] . '/out/state.json';
            $jsonEncode = new \Symfony\Component\Serializer\Encoder\JsonEncode();
            file_put_contents($outputStateFile, $jsonEncode->encode($result['state'], JsonEncoder::FORMAT));
        }
    }
    $app['logger']->log('info', 'Extractor finished successfully.');
    exit(0);
} catch (UserException|ConfigUserException $e) {
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
