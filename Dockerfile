FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Miro Cillik <miro@keboola.com>

# Install dependencies
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-devel php-pgsql; yum clean all
# Install psql client
RUN yum install -y postgresql postgresql-contrib sudo; yum clean all

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction

CMD php ./run.php --data=/data