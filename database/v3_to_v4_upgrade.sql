DROP TABLE IF EXISTS madweight;
DROP TABLE IF EXISTS madweightrun;
DROP TABLE IF EXISTS weight;
DROP TABLE IF EXISTS event;

CREATE TABLE analysis
(
    analysis_id int NOT NULL AUTO_INCREMENT,
    description text,
    cadiline tinytext,
    contact tinytext,
    PRIMARY KEY (analysis_id)
) ENGINE = INNODB;

ALTER TABLE result ADD analysis_id int NULL;
ALTER TABLE result ADD elog varchar(255) NULL;
ALTER TABLE result ADD CONSTRAINT FOREIGN KEY (analysis_id) REFERENCES analysis(analysis_id);
