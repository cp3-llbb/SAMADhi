FROM php:7.0-apache
RUN apt-get update && apt-get install git -y
RUN docker-php-ext-install mysqli && docker-php-ext-enable mysqli
RUN apt-get update && apt-get install -y gnupg2
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs build-essential
RUN npm install -g npm gulp bower
COPY config/php.ini /usr/local/etc/php/
COPY ./html/ /var/www/html/
RUN git clone https://github.com/shannah/xataface.git /var/www/html/xataface
WORKDIR /var/www/html/dashboard
RUN ./installDeps.sh
WORKDIR /var/www/html
