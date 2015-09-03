<?php
/**
 * File: index.php
 * Description:
 * -------------
 *
 * This is an entry file for this Dataface Application.  To use your application
 * simply point your web browser to this file.
 */
date_default_timezone_set('Europe/Brussels');
require_once '/var/www/html/xataface-2.0.3/public-api.php';
df_init(__FILE__, 'http://localhost/xataface-2.0.3')->display();
