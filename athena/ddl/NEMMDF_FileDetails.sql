CREATE EXTERNAL TABLE IF NOT EXISTS meter_poc.NEMMDF_FileDetails (
  `ID` string,
  `VersionHeader` string,
  `DateTime` timestamp,
  `FromParticipant` string,
  `ToParticipant` string,
  `source_file_id` string
) PARTITIONED BY (
  year int,
  month int,
  day int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://meterloader.poc/athena/NEMMDF_FileDetails/'
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');
