# Hive examples

Tasks:

1) Write custom UDF which can parse any user agent (UA) string into separate fields 
2) Use data from you UDF and find most popular device, browser, OS for each city.(I suppose it is ranking)



Put data to hdfs and list that all is correct.Data sets can be used from previous task.
----------

    
    hdfs dfs -mkdir /hive_task3/city
    hdfs dfs -mkdir /hive_task3/bid
    
    hdfs dfs -put /my_app/city.en.txt /hive_task3/city
    hdfs dfs -put /my_app/imp.20131019.txt /hive_task3/bid
    
Upload jar with UDF function
--------
     --- Start hive CLI
    [root@sandbox /]# hive
    log4j:WARN No such property [maxFileSize] in org.apache.log4j.DailyRollingFileAppender.
     Logging initialized using configuration in file:/etc/hive/2.6.0.3-8/0/hive-log4j.properties
    
     --- Add jar with custom UDF
    hive>
     >
     >
     >
     > ADD JAR /my_app/customUdf.jar;
    Added [/my_app/customUdf.jar] to class path
    Added resources: [/my_app/customUdf.jar]
    
    --- Check jar added
    hive> list jars;
    /my_app/customUdf.jar
    
    --- Create function that will be assigned to your implementation
    hive> CREATE TEMPORARY FUNCTION parseUA AS 'com.hive.udf.ParseUserAgent';
    OK
    Time taken: 1.272 seconds
    
    --- Check that invalid data parse return null
    hive> select parseUA('ata');
    OK
    NULL
    Time taken: 1.621 seconds, Fetched: 1 row(s)
    
    --- Check that valid data parse return array of correct data
    hive> select parseUA('Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0');
    OK
    ["Computer","Windows XP","Chrome","Browser"]
    Time taken: 0.66 seconds, Fetched: 1 row(s)
    
Start hive scripts for task
----------
    [root@sandbox /]# hive -f /most_popular_browser.sql
    [root@sandbox /]# hive -f /most_popular_device.sql
    [root@sandbox /]# hive -f /most_popular_os.sql
   
