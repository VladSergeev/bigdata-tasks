--create external table for getting data from csv

CREATE EXTERNAL TABLE IF NOT EXISTS city (code STRING, name STRING)
COMMENT 'Table for eng city'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/hive_task3/city';

--create external table for getting data from csv
CREATE EXTERNAL TABLE IF NOT EXISTS bid(
  bidId STRING,
  benchmark BIGINT,
  logType BIGINT,
  iPinYouID STRING,
  userAgent STRING,
  ip_addr STRING,
  regionId STRING,
  cityId STRING,
  adExchange STRING,
  domain STRING,
  url STRING,
  anonUrl STRING,
  adSlotId STRING,
  adSlotWidth STRING,
  adSlotHeight STRING,
  adSlotVisibility STRING,
  adSlotFormat STRING,
  adSlotFloorPrice STRING,
  creativeId STRING,
  bidingPrice DOUBLE,
  payingPrice DOUBLE,
  landingPageUrl STRING,
  advertiserId STRING,
  userProfileIds STRING)
  COMMENT 'Table for bid infornation'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/hive_task3/bid';

SET tez.runtime.compress=false;
ADD JAR /my_app/customUdf.jar;
CREATE TEMPORARY FUNCTION parseUA AS 'com.hive.udf.ParseUserAgent';

CREATE TABLE IF NOT EXISTS city_device_count(
  city STRING,
  device STRING,
  device_count BIGINT)
  COMMENT 'Pre-aggregation table for devices'
  STORED AS ORC;

INSERT OVERWRITE TABLE city_device_count
SELECT
  city_device.city,
  city_device.device,
  COUNT(city_device.device)
FROM(SELECT
       cit.name as city,
       parseUA(bidding.useragent)[0] as device
     FROM bid bidding JOIN city cit ON cit.code=bidding.cityId
    ) city_device
WHERE city_device.device!='NULL'
GROUP BY city_device.city,city_device.device;



select
city,
device,
rank() OVER ( partition by city order by device_count DESC)
from city_device_count;