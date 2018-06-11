FROM php:7.0-apache
RUN apt-get update && apt-get install git -y
RUN docker-php-ext-install mysqli && docker-php-ext-enable mysqli
COPY config/php.ini /usr/local/etc/php/
COPY ./html/ /var/www/html/
RUN git clone https://github.com/shannah/xataface.git /var/www/html/xataface

