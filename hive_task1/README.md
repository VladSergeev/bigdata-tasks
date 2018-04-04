# Hive examples

Tasks:

1) Put dataset on HDFS and explore it through Hive.
2) Count total number of flights per carrier in 2007.
3) The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports).
4) Find five most busy airports in US during Jun 01 - Aug 31.
5) Find the carrier who served the biggest number of flights.

Put data to hdfs and list that all is correct
----------

    hdfs dfs -mkdir /hive_task1/2007
    hdfs dfs -mkdir /hive_task1/airports
    hdfs dfs -mkdir /hive_task1/carriers
    hdfs dfs -ls /
    hdfs dfs -put /my_app/2007.csv /hive_task1/2007/2007.csv
    hdfs dfs -put /my_app/airports.csv /hive_task1/airports/airports.csv
    hdfs dfs -put /my_app/carriers.csv /hive_task1/carriers/carriers.csv
    hdfs dfs -ls /

Start tasks: firstly initialize.sql, then others
---------
    hive -f /my_app/initialize.sql
    hive -f /my_app/busy_airport_in_us.sql
    hive -f /my_app/carrier_with_biggest_number_of_flights.sql
    hive -f /my_app/count_flights_per_carrier.sql
    hive -f /my_app/flights_on_jun_with_airport.sql
   

Get result and fpain of SQL :) 
----------
    [root@sandbox /]# hive -f /my_app/busy_airport_in_us.sql
    log4j:WARN No such property [maxFileSize] in org.apache.log4j.DailyRollingFileAppender.
    
    Logging initialized using configuration in file:/etc/hive/2.6.0.3-8/0/hive-log4j.properties
    Query ID = root_20170906133920_e5ba6e9d-3a00-4a62-9d4b-667942750901
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1504622710986_0012)
    
    --------------------------------------------------------------------------------
            VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
    --------------------------------------------------------------------------------
    Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
    Map 6 ..........   SUCCEEDED      1          1        0        0       0       0
    Map 8 ..........   SUCCEEDED      1          1        0        0       0       0
    Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
    Reducer 4 ......   SUCCEEDED      1          1        0        0       0       0
    Reducer 5 ......   SUCCEEDED      1          1        0        0       0       0
    Reducer 7 ......   SUCCEEDED      1          1        0        0       0       0
    --------------------------------------------------------------------------------
    VERTICES: 07/07  [==========================>>] 100%  ELAPSED TIME: 15.76 s
    --------------------------------------------------------------------------------
    OK
    William B Hartsfield-Atlanta Intl       215345
    Chicago O'Hare International    184225
    Dallas-Fort Worth International 147336
    Denver Intl     124295
    Los Angeles International       121493
    Time taken: 21.513 seconds, Fetched: 5 row(s)