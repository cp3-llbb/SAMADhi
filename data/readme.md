example command to start the database:
chcon -Rt svirt_sandbox_file_t .
docker run --name samadhi-mysql -e MYSQL_ROOT_PASSWORD=mpp_mysql -e MYSQL_DATABASE=llbb -v $(pwd):/docker-entrypoint-initdb.d -d mysql:8.0.11 --default-authentication-plugin=mysql_native_password

Before running this, you should remove redundant sql files (upgrade.sql files should not harm but are not needed).
SAMADhi.sql can also be replaced by a backup file.
Note that sql files are executed in alphabetical order.
