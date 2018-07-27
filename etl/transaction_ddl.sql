DROP TABLE default.Datathon_Transactions

CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Transactions (
  `Mode` int,
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
) LOCATION 's3://stevan-melbourne-datathon/Transactions/'
TBLPROPERTIES ('has_encrypted_data'='false');


-- example of adding partitions to table.
ALTER TABLE default.datathon_transactions ADD
    PARTITION (year=2015, week=29) LOCATION 's3://stevan-melbourne-datathon/Transactions/Year=2015/Week=29/'
    PARTITION (year=2015, week=30) LOCATION 's3://stevan-melbourne-datathon/Transactions/Year=2015/Week=30/'

select *
from default.Datathon_Transactions
