-- Upgrade SAMADhi from v2 to v3
-- Add a `processed_lumi` column to the sample table

-- Alter sample table
ALTER TABLE sample ADD processed_lumi mediumtext;
