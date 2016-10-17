-- Upgrade SAMADhi from v4 to v5
-- Change sample.`event_weight_sum` column to DOUBLE
-- Change file.`event_weight_sum` column to DOUBLE

-- Alter sample table
ALTER TABLE sample MODIFY event_weight_sum DOUBLE;
ALTER TABLE file MODIFY event_weight_sum DOUBLE;
