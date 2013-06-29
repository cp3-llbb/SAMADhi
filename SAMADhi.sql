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
DESC dataset;

DROP TABLE IF EXISTS sample;
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
DESC sample;
