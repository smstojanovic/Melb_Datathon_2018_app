CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Transactions (
  `Mode` smallint,
  `BusinessDate` string,
  `DateTime` timestamp,
  `CardID` bigint,
  `Card_Type_ID` int,
  `VehicleID` int,
  `ParentRoute` string,
  `RouteID` int,
  `StopID` bigint ,
  `Scan_Type` string
) PARTITIONED BY (
  Year int,
  Week int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://ap-southeast-2/stevan-melbourne-datathon/Transactions/'
TBLPROPERTIES ('has_encrypted_data'='false');
