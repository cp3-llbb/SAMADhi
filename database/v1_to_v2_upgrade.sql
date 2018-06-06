-- Upgrade SAMADhi from v1 to v2
-- Add a `file` table
-- Add a `event_weight_sum` column to the sample table
-- Change default engine from MyISAM to InnoDB

-- Change engine
ALTER TABLE weight ENGINE=InnoDB;
ALTER TABLE madweightrun ENGINE=InnoDB;
ALTER TABLE madweight ENGINE=InnoDB;
ALTER TABLE event ENGINE=InnoDB;
ALTER TABLE sampleresult ENGINE=InnoDB;
ALTER TABLE result ENGINE=InnoDB;
ALTER TABLE sample ENGINE=InnoDB;
ALTER TABLE dataset ENGINE=InnoDB;
ALTER TABLE users ENGINE=InnoDB;

-- Alter sample table
ALTER TABLE sample ADD event_weight_sum float NOT NULL DEFAULT 1.0;

-- Create `file` table
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
