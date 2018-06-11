#!/bin/sh

# install sb-admin-2 and highcharts
git clone https://github.com/BlackrockDigital/startbootstrap-sb-admin-2.git
cd startbootstrap-sb-admin-2/
npm install
gulp
bower update --allow-root
gulp copy
npm install highcharts
cp -r node_modules/highcharts vendor/

# move everything in place
cd ..
mv startbootstrap-sb-admin-2/vendor .
mv startbootstrap-sb-admin-2/dist/css/* css/
mv startbootstrap-sb-admin-2/dist/js/* js/
rm -rf startbootstrap-sb-admin-2

