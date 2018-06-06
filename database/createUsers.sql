CREATE USER 'llbb'@'%' IDENTIFIED BY 'ih[DAMAS]-bbll';
CREATE USER 'llbb'@'localhost' IDENTIFIED BY 'ih[DAMAS]-bbll';
GRANT ALL PRIVILEGES ON `llbb`.* TO 'llbb'@'localhost';
GRANT ALL PRIVILEGES ON `llbb`.* TO 'llbb'@'%';
