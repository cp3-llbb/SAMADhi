DROP TABLE dataset;
CREATE TABLE dataset
(
name varchar(255) NOT NULL,
nevents int,
dsize int,
process varchar(255),
xsection float,
cmssw_release varchar(255),
globaltag varchar(255),
datatype varchar(255) NOT NULL,
energy float,
user_comment varchar(1024),
PRIMARY KEY (name)
);
DESC dataset;

