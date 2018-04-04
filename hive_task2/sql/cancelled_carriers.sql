--create external table for getting data from csv

CREATE EXTERNAL TABLE IF NOT EXISTS carrier_ext(CODE STRING, DESCRIPTION STRING)
COMMENT 'Table for carriers, all data from csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/hive_task1/carriers'
tblproperties ("skip.header.line.count"="1");

--create table with clean data from external table

CREATE TABLE IF NOT EXISTS carrier(CODE STRING, DESCRIPTION STRING)
COMMENT 'Main table for carriers'
STORED AS ORC;

-- merge data from external table to main excluding quotes

INSERT OVERWRITE TABLE carrier SELECT regexp_replace(CODE,"\"",""),regexp_replace(DESCRIPTION,"\"","") FROM CARRIER_EXT;



--create external table for getting data from csv

CREATE EXTERNAL TABLE IF NOT EXISTS airport_ext(
iata STRING,
airport STRING,
city STRING,
state STRING,
country STRING,
latitude DOUBLE ,
longitude DOUBLE)
COMMENT 'Table for airports, all data from csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/hive_task1/airports'
tblproperties ("skip.header.line.count"="1");

--create table with clean data from external table

CREATE TABLE IF NOT EXISTS airport(
iata STRING,
airport STRING,
city STRING,
state STRING,
country STRING,
latitude DOUBLE ,
longitude DOUBLE)
COMMENT 'Main table for airports'
STORED AS ORC;

-- merge data from external table to main excluding quotes

INSERT OVERWRITE TABLE airport
SELECT
regexp_replace(iata,"\"",""),
regexp_replace(airport,"\"",""),
regexp_replace(city,"\"",""),
regexp_replace(state,"\"",""),
regexp_replace(country,"\"",""),
latitude,
longitude
FROM airport_ext;

--create external table for getting data from csv

CREATE EXTERNAL TABLE IF NOT EXISTS flight_2007(
Year BIGINT,
Month BIGINT,
DayofMonth BIGINT,
DayOfWeek BIGINT,
DepTime BIGINT,
CRSDepTime BIGINT,
ArrTime BIGINT,
CRSArrTime BIGINT,
UniqueCarrier STRING,
FlightNum BIGINT,
TailNum STRING,
ActualElapsedTime BIGINT,
CRSElapsedTime BIGINT,
AirTime BIGINT,
ArrDelay BIGINT,
DepDelay BIGINT,
Origin STRING,
Dest STRING,
Distance BIGINT,
TaxiIn BIGINT,
TaxiOut BIGINT,
Cancelled BIGINT,
CancellationCode STRING,
Diverted BIGINT,
CarrierDelay BIGINT,
WeatherDelay BIGINT,
NASDelay BIGINT,
SecurityDelay BIGINT,
LateAircraftDelay BIGINT)
COMMENT 'Table for flights, all data from csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/hive_task1/2007'
tblproperties ("skip.header.line.count"="1");

SELECT
  carr.DESCRIPTION,
  carrier_with_cancel.counter,
  carrier_with_cancel.cities
  from (
        SELECT
            flight.UniqueCarrier as code,
            COUNT(UniqueCarrier) as counter,
            concat_ws(',',collect_set(flight.Origin)) AS cities
        from flight_2007 flight
        WHERE flight.Cancelled!=0
        group by flight.UniqueCarrier
  )carrier_with_cancel join carrier carr on (carrier_with_cancel.code=carr.CODE AND carrier_with_cancel.counter>1)
  ORDER BY carrier_with_cancel.counter DESC;