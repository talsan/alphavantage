CREATE EXTERNAL TABLE `av_prices`(
  `ticker` string,
  `download_timestamp` timestamp,
  `asofdate` date,
  `open` float,
  `high` float,
  `low` float,
  `close` float,
  `adjusted_close` float,
  `volume` float,
  `dividend_amount` float,
  `split_coefficient` float,
  `divyld` float,
  `return` float)
PARTITIONED BY (
  `rundate` date)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://alpha-vantage-api/type=pricing/state=formatted/period=daily'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'last_modified_by'='hadoop',
  'last_modified_time'='1592176665',
  'skip.header.line.count'='1',
  'transient_lastDdlTime'='1592176665')


CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.av_invalid_tickers (
  `ticker` string,
  `upload_timestamp` timestamp

) PARTITIONED BY (
  rundate date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://alpha-vantage-api/type=pricing/state=invalid_tickers/'
TBLPROPERTIES ('has_encrypted_data'='false',
               'skip.header.line.count'='1');