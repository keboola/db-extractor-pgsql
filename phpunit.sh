#!/usr/bin/env bash

composer install -n;

export ROOT_PATH="/code";

./vendor/bin/phpunit;
