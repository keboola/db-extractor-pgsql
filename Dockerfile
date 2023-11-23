FROM php:8.2-cli-buster

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    zip \
    libpq-dev \
    postgresql \
    postgresql-contrib \
    libicu-dev \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO pgsql
RUN docker-php-ext-install pdo pdo_pgsql pgsql

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

# Disable PgSQL server side debugging mesages.
#
# The database server can generate debug messages and send them to the client.
# It cannot be configured in PHP, the message goes from libpg -> pgsql extension -> PDO -> PHP STDOUT.
# Azure Managed PgSQL instances generate "LOG" level messages through this channel.
# The "LOG" messages then breaks STDOUT of the testConnection synchronous action, it is no more valid JSON.
#
# This behavior can be modified via "client_min_messages" setting, default value is "NOTICE" level.
# The setting can be set via SQL, using "SET LOCAL ...", but that's too late, some messages are logged when creating a new connection.
# Fortunately, libpq can be configured via "PGOPTIONS" environment variable.
#
# Read more:
# - https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-SHELL "20.1.4. Parameter Interaction via the Shell"
# - https://www.postgresql.org/docs/16/runtime-config-client.html "20.11. Client Connection Defaults / client_min_messages"
ENV PGOPTIONS="-c client_min_messages=ERROR"

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
COPY patches /code/patches

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY . /code/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD php ./src/run.php
