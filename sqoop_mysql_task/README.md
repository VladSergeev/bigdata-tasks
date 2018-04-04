# Sqoop to Mysql

Tasks:

Upload the weather data into your HDP sandbox's HDFS in directory data.
Use sqoop to export all the data to MySQL(normally the credentials are root / hadoop).
Result of the following queries run in MySQL:

    SELECT count(*) FROM weather;
    SELECT * FROM weather ORDER BY stationid, date LIMIT 10;



Put data to hdfs and list that all is correct.Data sets can be used from previous task.
----------

    [root@sandbox my_homework]# hdfs dfs -mkdir /sqoop_task
    [root@sandbox my_homework]# hdfs dfs -put /better-format /sqoop_task
    [root@sandbox my_homework]# hdfs dfs -ls /sqoop_task/better-format
    Found 13 items
    -rw-r--r--   1 root hdfs          0 2018-01-31 13:20 /sqoop_task/better-format/_SUCCESS
    -rw-r--r--   1 root hdfs  297416812 2018-01-31 13:20 /sqoop_task/better-format/part-00000
    -rw-r--r--   1 root hdfs  297415975 2018-01-31 13:20 /sqoop_task/better-format/part-00001
    -rw-r--r--   1 root hdfs  297410274 2018-01-31 13:21 /sqoop_task/better-format/part-00002
    -rw-r--r--   1 root hdfs  297412771 2018-01-31 13:21 /sqoop_task/better-format/part-00003
    -rw-r--r--   1 root hdfs  297418320 2018-01-31 13:21 /sqoop_task/better-format/part-00004
    -rw-r--r--   1 root hdfs  297416863 2018-01-31 13:21 /sqoop_task/better-format/part-00005
    -rw-r--r--   1 root hdfs  297409067 2018-01-31 13:21 /sqoop_task/better-format/part-00006
    -rw-r--r--   1 root hdfs  297418245 2018-01-31 13:21 /sqoop_task/better-format/part-00007
    -rw-r--r--   1 root hdfs  297419400 2018-01-31 13:22 /sqoop_task/better-format/part-00008
    -rw-r--r--   1 root hdfs  297417020 2018-01-31 13:22 /sqoop_task/better-format/part-00009
    -rw-r--r--   1 root hdfs  297415274 2018-01-31 13:22 /sqoop_task/better-format/part-00010
    -rw-r--r--   1 root hdfs  297411671 2018-01-31 13:22 /sqoop_task/better-format/part-00011
    
Prepare MySql (user = root, pas= hadoop)
--------
     [root@sandbox my_homework]# mysql -u root -p
     Enter password:
     Welcome to the MySQL monitor.  Commands end with ; or \g.
     Your MySQL connection id is 1294
     Server version: 5.6.36 MySQL Community Server (GPL)
     
     Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.
     
     Oracle is a registered trademark of Oracle Corporation and/or its
     affiliates. Other names may be trademarks of their respective
     owners.
     
     Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
     
     mysql> create database sqoopDb;
     Query OK, 1 row affected (0.01 sec)
     
     mysql> use sqoopDb;
     Database changed
     mysql> create table weather(stationId integer, date DATE, tmin integer, tmax integer, snow varchar(60), snwd varchar(60), prcp varchar(60));
     Query OK, 0 rows affected (0.68 sec)
    
     mysql> exit;
     
Note ff you will get this error:

     com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: Access denied for user ''@'localhost' to database 'sqoopDb' 

Then:
     
     mysql> grant all privileges on sqoopDb.* to ''@localhost ;
     Query OK, 0 rows affected (0.17 sec)

Start SQOOP import
----------
    [root@sandbox my_homework]# sqoop export --connect jdbc:mysql://localhost/sqoopDb --table weather -m 1 --export-dir /sqoop_task/better-format
    Warning: /usr/hdp/2.6.0.3-8/accumulo does not exist! Accumulo imports will fail.
    Please set $ACCUMULO_HOME to the root of your Accumulo installation.
    18/02/01 11:57:12 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6.2.6.0.3-8
    18/02/01 11:57:12 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
    18/02/01 11:57:12 INFO tool.CodeGenTool: Beginning code generation
    18/02/01 11:57:12 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `weather` AS t LIMIT 1
    18/02/01 11:57:13 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `weather` AS t LIMIT 1
    18/02/01 11:57:13 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/hdp/2.6.0.3-8/hadoop-mapreduce
    Note: /tmp/sqoop-root/compile/907d0f64f56712b7e210560e945677d3/weather.java uses or overrides a deprecated API.
    Note: Recompile with -Xlint:deprecation for details.
    18/02/01 11:57:14 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-root/compile/907d0f64f56712b7e210560e945677d3/weather.jar
    18/02/01 11:57:14 INFO mapreduce.ExportJobBase: Beginning export of weather
    18/02/01 11:57:15 INFO client.RMProxy: Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8032
    18/02/01 11:57:15 INFO client.AHSProxy: Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
    18/02/01 11:57:24 INFO input.FileInputFormat: Total input paths to process : 12
    18/02/01 11:57:24 INFO input.FileInputFormat: Total input paths to process : 12
    18/02/01 11:57:25 INFO mapreduce.JobSubmitter: number of splits:1
    18/02/01 11:57:25 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1517403271527_0002
    18/02/01 11:57:26 INFO impl.YarnClientImpl: Submitted application application_1517403271527_0002
    18/02/01 11:57:26 INFO mapreduce.Job: The url to track the job: http://sandbox.hortonworks.com:8088/proxy/application_1517403271527_0002/
    18/02/01 11:57:26 INFO mapreduce.Job: Running job: job_1517403271527_0002
    18/02/01 11:57:38 INFO mapreduce.Job: Job job_1517403271527_0002 running in uber mode : false
    18/02/01 11:57:38 INFO mapreduce.Job:  map 0% reduce 0%
    18/02/01 11:58:00 INFO mapreduce.Job:  map 1% reduce 0%
    ....
    18/02/01 12:47:30 INFO mapreduce.Job:  map 98% reduce 0%
    18/02/01 12:48:00 INFO mapreduce.Job:  map 99% reduce 0%
    18/02/01 12:48:33 INFO mapreduce.Job:  map 100% reduce 0%
    18/02/01 12:48:49 INFO mapreduce.Job: Job job_1517403271527_0002 completed successfully
    18/02/01 12:48:49 INFO mapreduce.Job: Counters: 30
            File System Counters
                    FILE: Number of bytes read=0
                    FILE: Number of bytes written=165949
                    FILE: Number of read operations=0
                    FILE: Number of large read operations=0
                    FILE: Number of write operations=0
                    HDFS: Number of bytes read=3572130760
                    HDFS: Number of bytes written=0
                    HDFS: Number of read operations=109
                    HDFS: Number of large read operations=0
                    HDFS: Number of write operations=0
            Job Counters
                    Launched map tasks=1
                    Data-local map tasks=1
                    Total time spent by all maps in occupied slots (ms)=3067192
                    Total time spent by all reduces in occupied slots (ms)=0
                    Total time spent by all map tasks (ms)=3067192
                    Total vcore-milliseconds taken by all map tasks=3067192
                    Total megabyte-milliseconds taken by all map tasks=766798000
            Map-Reduce Framework
                    Map input records=81424802
                    Map output records=81424802
                    Input split bytes=3232
                    Spilled Records=0
                    Failed Shuffles=0
                    Merged Map outputs=0
                    GC time elapsed (ms)=12735
                    CPU time spent (ms)=309130
                    Physical memory (bytes) snapshot=173072384
                    Virtual memory (bytes) snapshot=2145320960
                    Total committed heap usage (bytes)=46661632
            File Input Format Counters
                    Bytes Read=0
            File Output Format Counters
                    Bytes Written=0
    18/02/01 12:48:49 INFO mapreduce.ExportJobBase: Transferred 3.3268 GB in 3,094.3262 seconds (1.1009 MB/sec)
    18/02/01 12:48:49 INFO mapreduce.ExportJobBase: Exported 81424802 records.
   
Verify Mysql table
----------
    mysql> SELECT count(*) FROM weather;
    +----------+
    | count(*) |
    +----------+
    | 81424802 |
    +----------+
    1 row in set (59.12 sec)
    
    mysql> SELECT * FROM weather ORDER BY stationid, date LIMIT 10;
    +-------------+------------+------+------+------+------+------+
    | stationId   | date       | tmin | tmax | snow | snwd | prcp |
    +-------------+------------+------+------+------+------+------+
    | AG000060390 | 1940-01-01 | 47   | 224  | NULL | NULL | 0    |
    | AG000060390 | 1940-01-02 | 88   | 202  | NULL | NULL | 0    |
    | AG000060390 | 1940-01-03 | 110  | 210  | NULL | NULL | 18   |
    | AG000060390 | 1940-01-04 | 98   | 191  | NULL | NULL | 185  |
    | AG000060390 | 1940-01-05 | 100  | 175  | NULL | NULL | NULL |
    | AG000060390 | 1940-01-06 | 120  | 173  | NULL | NULL | NULL |
    | AG000060390 | 1940-01-07 | 85   | 172  | NULL | NULL | 0    |
    | AG000060390 | 1940-01-08 | 60   | 208  | NULL | NULL | 20   |
    | AG000060390 | 1940-01-09 | 96   | 138  | NULL | NULL | 304  |
    | AG000060390 | 1940-01-10 | 72   | 136  | NULL | NULL | 205  |
    +-------------+------------+------+------+------+------+------+
    10 rows in set (2 min 8.26 sec)