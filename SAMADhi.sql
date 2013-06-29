DROP TABLE IF EXISTS dataset;
CREATE TABLE dataset
(
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
PRIMARY KEY (name)
);
DESC dataset;

