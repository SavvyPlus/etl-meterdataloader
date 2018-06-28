-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_IntervalData (
  `StreamID` string,
  `IntervalDate` TIMESTAMP,
  `IntervalNumber` int,
  `IntervalLength`, int,
  `Value` double,
  `UOM_ID`, string,
  `QualityDetailsID`, string
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
