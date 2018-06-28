-- Done
CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.SPMDF_MeterDetails (
  `MeterRef` string,
  `NMI` string,
  `StreamRef` string,
  `MeterSerialNumber` string,
  `DLF_Code` string,
  `TNI_Code` string,
  `DLF_Value` string,
  `TLF_Value` string,
  `Max_Date` date
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
