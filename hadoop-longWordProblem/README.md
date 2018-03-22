Detect long words
----------
For testing you can use data from test resources.

Put data to hdfs and list that all is correct
----------

    hdfs dfs -put /my_app/mrsandman.txt /user/hadoop1/mrsandman.txt
    hdfs dfs -ls /user/hadoop1


Run Long word count job
----------

    hadoop jar /my_app/longWordProblem.jar com.hadoop.task1.LongWordApp /user/hadoop1/mrsandman.txt /user/hadoop1/output

Expected result for job log 
----------

    17/08/14 11:59:04 INFO client.RMProxy: Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8032
    17/08/14 11:59:04 INFO client.AHSProxy: Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
    17/08/14 11:59:05 INFO input.FileInputFormat: Total input paths to process : 1
    17/08/14 11:59:05 INFO mapreduce.JobSubmitter: number of splits:1
    17/08/14 11:59:05 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1502707477586_0002
    17/08/14 11:59:05 INFO impl.YarnClientImpl: Submitted application application_1502707477586_0002
    17/08/14 11:59:05 INFO mapreduce.Job: The url to track the job: http://sandbox.hortonworks.com:8088/proxy/application_1502707477586_0002/
    17/08/14 11:59:05 INFO mapreduce.Job: Running job: job_1502707477586_0002
    17/08/14 11:59:11 INFO mapreduce.Job: Job job_1502707477586_0002 running in uber mode : false
    17/08/14 11:59:11 INFO mapreduce.Job:  map 0% reduce 0%
    17/08/14 11:59:16 INFO mapreduce.Job:  map 100% reduce 0%
    17/08/14 11:59:21 INFO mapreduce.Job:  map 100% reduce 100%
    17/08/14 11:59:22 INFO mapreduce.Job: Job job_1502707477586_0002 completed successfully
    17/08/14 11:59:22 INFO mapreduce.Job: Counters: 49
        File System Counters
                FILE: Number of bytes read=26
                FILE: Number of bytes written=298947
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=1063
                HDFS: Number of bytes written=17
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=2362
                Total time spent by all reduces in occupied slots (ms)=2565
                Total time spent by all map tasks (ms)=2362
                Total time spent by all reduce tasks (ms)=2565
                Total vcore-milliseconds taken by all map tasks=2362
                Total vcore-milliseconds taken by all reduce tasks=2565
                Total megabyte-milliseconds taken by all map tasks=590500
                Total megabyte-milliseconds taken by all reduce tasks=641250
        Map-Reduce Framework
                Map input records=27
                Map output records=1
                Map output bytes=18
                Map output materialized bytes=26
                Input split bytes=127
                Combine input records=1
                Combine output records=1
                Reduce input groups=1
                Reduce shuffle bytes=26
                Reduce input records=1
                Reduce output records=1
                Spilled Records=2
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=118
                CPU time spent (ms)=740
                Physical memory (bytes) snapshot=325816320
                Virtual memory (bytes) snapshot=4263440384
                Total committed heap usage (bytes)=154664960
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=936
        File Output Format Counters
                Bytes Written=17


Cat result of input file 
----------
    [root@sandbox /]# hadoop fs -cat /user/hadoop1/keeprollin.txt
    Alright partner
    Keep on rollin' baby
    You know what time it is
    
    Throw your hands up
    Ladies and gentlemen
    Chocolate Starfish
    Keep on rolling baby
    Move in, now move out
    Hands up, now hands down
    Back up, back up
    Tell me what you're gonna do now
    Breath in, now breath out
    Hands up, now hands down
    Back up, back up
    Tell me what you're gonna do now
    etc...
    
Cat result of MR job 
----------

        [root@sandbox /]# hadoop fs -cat /user/hadoop1/output/part-r-00000
        10      themselves
