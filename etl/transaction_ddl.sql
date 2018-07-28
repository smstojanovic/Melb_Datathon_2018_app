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


-- master data

CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Stop_Locations (
  `StopID` bigint,
  `street_name` string,
  `street_corner` string,
  `location_type` string,
  `suburb` string,
  `postcode` double,
  `city` string,
  `region` string,
  `region_type` string,
  `lat` double,
  `lng` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://stevan-melbourne-datathon/Stop_Locations/'
TBLPROPERTIES ('has_encrypted_data'='false');


CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Card_Types (
  `Card_Type_ID` bigint,
  `Card_Description` string,
  `Card_Payment_Type` string,
  `Card_Type` string,
  `Card_Type_Detail` string,
  `Card_Type_Description` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://stevan-melbourne-datathon/Card_Types/'
TBLPROPERTIES ('has_encrypted_data'='false');


CREATE EXTERNAL TABLE IF NOT EXISTS default.Datathon_Calendar (
  `YYYYMMDD` bigint,
  `YYYY-MM-DD` string,
  `YYYY` int,
  `Financial_Year` string,
  `Month_num_2y` int,
  `Month` string,
  `YYYYMM` int,
  `YYYYQQ` string,
  `Financial_Quarter` string,
  `unknown` int,
  `unknown_2` int,
  `Day_Type` string,
  `Day_Type_2` string,
  `Day_Type_Hol` string,
  `Day_of_Week_Num` int,
  `Day_of_Week` string,
  `YYYY2M` int,
  `unknown_3` string,
  `MM` int,
  `HHMM` int,
  `Week_Ending` string,
  `Quarter` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://stevan-melbourne-datathon/Calendar/'
TBLPROPERTIES ('has_encrypted_data'='false');
