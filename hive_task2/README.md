# Hive examples

Tasks:

1) Find all carriers who cancelled more than 1 flights during 2007, order them from biggest to lowest by number 
of cancelled flights and list in each record all departure cities where cancellation happened.
2) How many MR jobs where instanced for this query?



Put data to hdfs and list that all is correct.Data sets can be used from previous task.
----------

    hdfs dfs -mkdir /hive_task1/2007
    hdfs dfs -mkdir /hive_task1/airports
    hdfs dfs -mkdir /hive_task1/carriers
    hdfs dfs -ls /
    hdfs dfs -put /my_app/2007.csv /hive_task1/2007/2007.csv
    hdfs dfs -put /my_app/airports.csv /hive_task1/airports/airports.csv
    hdfs dfs -put /my_app/carriers.csv /hive_task1/carriers/carriers.csv
    hdfs dfs -ls /

Start tasks scripts
---------
    hive -f /my_app/cancelled_carriers.sql
   
