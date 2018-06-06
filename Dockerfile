FROM php:7.0-apache
COPY ./html/ /var/www/html/
RUN apt-get update && apt-get install git -y && git clone https://github.com/shannah/xataface.git /var/www/html/xataface
COPY config/php.ini /usr/local/etc/php/
RUN docker-php-ext-install mysqli && docker-php-ext-enable mysqli

