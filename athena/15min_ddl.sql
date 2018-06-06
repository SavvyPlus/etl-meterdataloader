CREATE EXTERNAL TABLE IF NOT EXISTS poc.imd_15_test (
  `MeterRef` string,
  `Date` string,
  `PeriodID` int,
  `Net_KWH` double,
  `Net_KVARH` double,
  `Exp_KWH` double,
  `Imp_KWH` double,
  `Exp_KVARH` double,
  `Imp_KVARH` double,
  `KW` double,
  `KVA` double,
  `KVA15` double,
  `KW15` double,
  `source_file_id` string,
  `MDPUpdateDateTime` timestamp,
  `QualityCode` string,
  `QualityNumber` string
) PARTITIONED BY (
  year int,
  month int,
  day int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://meterloader.poc/athena/imd_15min/'
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');
