-- Upgrade SAMADhi from v3 to v4
-- Add a `extras_event_weight_sum` column to the sample table
-- Add a `extras_event_weight_sum` column to the file table

-- Alter sample table
ALTER TABLE sample ADD extras_event_weight_sum mediumtext;
ALTER TABLE file ADD extras_event_weight_sum mediumtext;
