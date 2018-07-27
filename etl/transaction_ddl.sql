DROP TABLE default.Datathon_Transactions

CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Transactions (
  `Mode` string,
  `BusinessDate` string,
  `DateTime` string,
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


ALTER TABLE default.datathon_transactions ADD
    PARTITION (year=2016, week=8) LOCATION 's3://stevan-melbourne-datathon/Transactions/Year=2018/Week=8/'

select *
from default.Datathon_Transactions
