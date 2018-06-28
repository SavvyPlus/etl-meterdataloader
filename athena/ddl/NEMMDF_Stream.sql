-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_Stream (
  `ID` string,
  `NMI` string,
  `NMISuffix` string,
  `MeterPointRef` string,
  `StreamRef` string
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
