DROP TABLE IF EXISTS weight;
DROP TABLE IF EXISTS madweightrun;
DROP TABLE IF EXISTS madweight;
DROP TABLE IF EXISTS event;
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

INSERT INTO users (`userName`,`password`,`role`) VALUES ('adminUser','050f02a6a1221639d03d1ad935ff7fbf','ADMIN');

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
extras_event_weight_sum mediumtext,
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

CREATE TABLE event
(
event_id BIGINT NOT NULL AUTO_INCREMENT,
event_number int NOT NULL,
run_number int NOT NULL,
dataset_id int NOT NULL,
PRIMARY KEY (event_id),
FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id)
) ENGINE = INNODB;

CREATE TABLE madweight
(
process_id int NOT NULL AUTO_INCREMENT,
name varchar(255) NOT NULL,
diagram varchar(255) NOT NULL,
isr int NOT NULL,
nwa int NOT NULL,
cm_energy float NOT NULL,
higgs_width float,
ident_mw_card text NOT NULL,
ident_card text NOT NULL,
info_card text NOT NULL,
MadWeight_card text NOT NULL,
mapping_card text NOT NULL,
param_card text NOT NULL,
param_card_1 text NOT NULL,
proc_card_mg5 text NOT NULL,
run_card text NOT NULL,
transfer_card text NOT NULL,
transfer_fctVersion varchar(255) NOT NULL,
transfer_function text NOT NULL,
PRIMARY KEY (process_id),
KEY idx_name (name)
) ENGINE = INNODB;

CREATE TABLE madweightrun
(
mwrun_id int NOT NULL AUTO_INCREMENT,
madweight_process int NOT NULL,
lhco_sample_id int NOT NULL,
systematics varchar(255),
version tinyint NOT NULL DEFAULT 1,
user_comment text,
creation_time timestamp,
PRIMARY KEY (mwrun_id),
UNIQUE INDEX (madweight_process,lhco_sample_id,systematics,version),
FOREIGN KEY (madweight_process) REFERENCES madweight(process_id),
FOREIGN KEY (lhco_sample_id) REFERENCES sample(sample_id)
) ENGINE = INNODB;

CREATE TABLE weight
(
weight_id BIGINT NOT NULL AUTO_INCREMENT,
event_id BIGINT NOT NULL,
madweight_run int NOT NULL,
value float,
uncertainty float,
PRIMARY KEY (weight_id),
UNIQUE INDEX (event_id,madweight_run),
FOREIGN KEY (event_id) REFERENCES event(event_id),
FOREIGN KEY (madweight_run) REFERENCES madweightrun(mwrun_id)
) ENGINE = INNODB;

CREATE TABLE file
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    sample_id int NOT NULL,
    lfn varchar(500) NOT NULL,
    pfn varchar(500) NOT NULL,
    event_weight_sum float,
    extras_event_weight_sum mediumtext,
    nevents BIGINT,
    PRIMARY KEY (id),
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id) ON DELETE CASCADE
) ENGINE = INNODB;
