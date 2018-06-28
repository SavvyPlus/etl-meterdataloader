-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_B2BDetailes (
  `ID` string,
  `StreamID` string,
  `IntervalDate` timestamp,
  `TransCode` string,
  `RetServiceOrder` string,
  `ReadDateTime` timestamp,
  `IndexRead` string
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
