DROP TABLE IF EXISTS sampleresult;
DROP TABLE IF EXISTS result;
DROP TABLE IF EXISTS sample;
DROP TABLE IF EXISTS dataset;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS file;

CREATE TABLE  users
(
userID INT( 11 ) NOT NULL AUTO_INCREMENT ,
userName VARCHAR( 32 ) NOT NULL ,
password VARCHAR( 32 ) NOT NULL ,
role ENUM('READ ONLY','NO ACCESS','EDIT','DELETE','USER','ADMIN') DEFAULT 'READ ONLY',
PRIMARY KEY (userID) ,
UNIQUE (userName)
) ENGINE = INNODB;

INSERT INTO 'users' (`userName`,`password`,`role`)
VALUES ('adminUser','050f02a6a1221639d03d1ad935ff7fbf','ADMIN');

CREATE TABLE dataset
(
dataset_id int NOT NULL AUTO_INCREMENT,
name varchar(255) NOT NULL,
datatype varchar(255) NOT NULL,
process varchar(255),
nevents int,
dsize bigint,
xsection float,
cmssw_release varchar(255),
globaltag varchar(255),
energy float,
creation_time datetime,
user_comment text,
PRIMARY KEY (dataset_id),
KEY idx_name (name)
) ENGINE = INNODB;

CREATE TABLE sample
(
sample_id int NOT NULL AUTO_INCREMENT,
name varchar(255) NOT NULL,
path varchar(255) NOT NULL,
sampletype varchar(255) NOT NULL,
nevents_processed int,
nevents int,
normalization float NOT NULL DEFAULT 1.0, 
event_weight_sum float NOT NULL DEFAULT 1.0,
processed_lumi mediumtext,
luminosity float,
code_version varchar(255),
user_comment text,
author tinytext,
creation_time timestamp,
source_dataset_id int,
source_sample_id int,
PRIMARY KEY (sample_id),
KEY idx_name (name),
FOREIGN KEY (source_dataset_id) REFERENCES dataset(dataset_id),
FOREIGN KEY (source_sample_id) REFERENCES sample(sample_id)
) ENGINE = INNODB;

CREATE TABLE result
(
result_id int NOT NULL AUTO_INCREMENT,
path varchar(255) NOT NULL,
description text,
author tinytext,
creation_time timestamp,
PRIMARY KEY (result_id),
KEY idx_path (path)
) ENGINE = INNODB;

CREATE TABLE sampleresult
(
sample_id int NOT NULL,
result_id int NOT NULL,
CONSTRAINT SR_ID PRIMARY KEY (sample_id,result_id),
FOREIGN KEY (sample_id) REFERENCES sample(sample_id),
FOREIGN KEY (result_id) REFERENCES result(result_id)
) ENGINE = INNODB;

CREATE TABLE file
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    sample_id int NOT NULL,
    lfn varchar(500) NOT NULL,
    pfn varchar(500) NOT NULL,
    event_weight_sum float,
    nevents BIGINT,
    PRIMARY KEY (id),
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id) ON DELETE CASCADE
) ENGINE = INNODB;
