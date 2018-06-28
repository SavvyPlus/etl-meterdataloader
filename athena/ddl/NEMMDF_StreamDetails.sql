-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_StreamDetails (
  `ID` string,
  `NMIConfiguration` string,
  `RegisterID` string,
  `MDMDataStreamIdentifier` string,
  `MeterSerialNumber` string,
  `UOM` string,
  `IntervalLength` int,
  `NextScheduledReadDate` string
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
