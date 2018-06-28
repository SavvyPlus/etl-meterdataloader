-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_StreamDay (
  `StreamID` string,
  `IntervalDate` TIMESTAMP,
  `source_file_id` string,
  `UpdateDateTime` TIMESTAMP,
  `MSATSLoadDateTime` TIMESTAMP,
  `StreamDetailsID` string,
  `IntervalLength` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
LOCATION ''
-- path to file
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');
