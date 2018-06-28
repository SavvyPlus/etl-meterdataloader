CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_UOM (
  `UOM_ID` int,
  `UOM` string,
  `Base_UOM` string,
  `Base_Multiplier` double,
  `Integrative` int,
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
