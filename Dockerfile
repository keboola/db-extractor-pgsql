FROM php:7.2
MAINTAINER Miro Cillik <miro@keboola.com>

ENV DEBIAN_FRONTEND noninteractive

ENV COMPOSER_ALLOW_SUPERUSER=1

RUN apt-get update \
  && apt-get install -y git ssh zip unzip libpq-dev --no-install-recommends \
  && docker-php-ext-install pdo pdo_pgsql pgsql

# Install psql
  # required to bypass https://github.com/debuerreotype/debuerreotype/issues/10
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man7 \
  && apt-get install -y postgresql postgresql-contrib --no-install-recommends

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

ADD . /code

RUN composer install --no-interaction

CMD php ./run.php --data=/data
