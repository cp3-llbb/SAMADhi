DROP TABLE IF EXISTS weight;
DROP TABLE IF EXISTS madweight;
DROP TABLE IF EXISTS event;
DROP TABLE IF EXISTS sampleresult;
DROP TABLE IF EXISTS result;
DROP TABLE IF EXISTS sample;
DROP TABLE IF EXISTS dataset;

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
);

CREATE TABLE sample
(
sample_id int NOT NULL AUTO_INCREMENT,
name varchar(255) NOT NULL,
path varchar(255) NOT NULL,
sampletype varchar(255) NOT NULL,
nevents_processed int NOT NULL,
nevents int,
normalization float DEFAULT 1.0,
luminosity float,
code_version varchar(255),
user_comment text,
source_dataset_id int,
source_sample_id int,
PRIMARY KEY (sample_id),
KEY idx_name (name)
);

CREATE TABLE result
(
result_id int NOT NULL AUTO_INCREMENT,
path varchar(255) NOT NULL,
description text,
PRIMARY KEY (result_id)
);

CREATE TABLE sampleresult
(
sample_id int NOT NULL,
result_id int NOT NULL,
CONSTRAINT SR_ID PRIMARY KEY (sample_id,result_id)
);

CREATE TABLE event
(
event_id BIGINT NOT NULL AUTO_INCREMENT,
event_number int NOT NULL,
run_number int NOT NULL,
dataset_id int NOT NULL,
PRIMARY KEY (event_id),
FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id)
);

CREATE TABLE madweight
(
process_id int NOT NULL AUTO_INCREMENT,
name varchar(255) NOT NULL,
diagram varchar(255) NOT NULL,
isr int NOT NULL,
systematics varchar(255),
card text NOT NULL,
PRIMARY KEY (process_id)
);

CREATE TABLE weight
(
weight_id BIGINT NOT NULL AUTO_INCREMENT,
event_id BIGINT NOT NULL,
madweight_process int NOT NULL,
value float,
uncertainty float,
version tinyint DEFAULT 1,
PRIMARY KEY (weight_id),
UNIQUE INDEX (event_id,madweight_process,version)
);

