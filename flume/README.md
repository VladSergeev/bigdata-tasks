# Flume

Task:

Use the following command to create and gradually grow the input: shell

    cat linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > output.txt

Write a Flume configuration to upload the ever growing output.txt file to HDFS.

Include report of the output of following command of your output.txt in HDFS:

    hdfs dfs cat ...

.
----------


Put data to hdfs and list that all is correct.Data sets can be used from previous task.
----------

    [root@sandbox my_homework]# hdfs dfs -mkdir /flume_task
    [root@sandbox my_homework]# hdfs dfs -put linux_messages_3000lines.txt /flume_task
    [root@sandbox my_homework]# hdfs dfs -ls /flume_task
    Found 1 items
    -rw-r--r--   1 root hdfs     253182 2018-02-01 13:29 /flume_task/linux_messages_3000lines.txt
    
Prepare Flume configuration
--------

     In ambari UI(http://localhost:8080/#/main/services/FLUME/configs) or in config file: 
     
     

     [root@sandbox /]# cat /usr/hdp/current/flume-server/conf/flume.conf
     # Licensed to the Apache Software Foundation (ASF) under one
     # or more contributor license agreements.  See the NOTICE file
     # distributed with this work for additional information
     # regarding copyright ownership.  The ASF licenses this file
     # to you under the Apache License, Version 2.0 (the
     # "License"); you may not use this file except in compliance
     # with the License.  You may obtain a copy of the License at
     #
     #  http://www.apache.org/licenses/LICENSE-2.0
     #
     # Unless required by applicable law or agreed to in writing,
     # software distributed under the License is distributed on an
     # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
     # KIND, either express or implied.  See the License for the
     # specific language governing permissions and limitations
     # under the License.
     
     
     # flume.conf: Add your flume configuration here and start flume
     #             Note if you are using the Windows srvice or Unix service
     #             provided by the HDP distribution, they will assume the
     #             agent's name in this file to be 'a1'
     #
     
     
     # Name the components on this agent
     agent.sources = file-source
     agent.sinks = hdfs-sink
     agent.channels = mem-channel
     
     # Associate channel with source and sink
     agent.sources.file-source.channels = mem-channel
     agent.sinks.hdfs-sink.channel = mem-channel
     
     # Configure the source
     agent.sources.file-source.type = spooldir
     agent.sources.file-source.spoolDir = /my_homework/test
     agent.sources.file-source.fileHeader = true
     
     # Configure the sink
     agent.sinks.hdfs-sink.type = hdfs
     agent.sinks.hdfs-sink.hdfs.path = /tmp/log.log
     agent.sinks.hdfs-sink.hdfs.fileType = DataStream
     agent.sinks.hdfs-sink.hdfs.path = /flume/test/
     
     # Use a channel which buffers events in memory
     agent.channels.mem-channel.type = memory
     agent.channels.mem-channel.capacity = 1000
     agent.channels.mem-channel.transactionCapacity = 100
     
Note that u should restart:

     bin/flume-ng agent -n $agent_name -c conf -f conf/flume-conf.properties.template
    

Then start shell script
----------
    cat linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > output.txt
   
You can see logs of flume server and agent 
----------
       tail -f /var/log/flume/flume.log
       tail -f /var/log/flume/flume-agent.log

You can see results that in directory /flume/test appear a lot of files and you can cat one of them
----------
       [root@sandbox /]# hdfs dfs -ls /flume/test
       Found 67 items
       -rw-r--r--   1 root hdfs       1004 2018-02-02 15:14 /flume/test/FlumeData.1517584482637
       -rw-r--r--   1 root hdfs        448 2018-02-02 15:14 /flume/test/FlumeData.1517584482638
       -rw-r--r--   1 root hdfs        385 2018-02-02 15:14 /flume/test/FlumeData.1517584482639
       -rw-r--r--   1 root hdfs        629 2018-02-02 15:14 /flume/test/FlumeData.1517584482640
       -rw-r--r--   1 root hdfs        302 2018-02-02 15:14 /flume/test/FlumeData.1517584482641
       -rw-r--r--   1 root hdfs        511 2018-02-02 15:14 /flume/test/FlumeData.1517584482642
       -rw-r--r--   1 root hdfs        476 2018-02-02 15:14 /flume/test/FlumeData.1517584482643
       -rw-r--r--   1 root hdfs        425 2018-02-02 15:14 /flume/test/FlumeData.1517584482644
       -rw-r--r--   1 root hdfs        528 2018-02-02 15:14 /flume/test/FlumeData.1517584482645
       -rw-r--r--   1 root hdfs        417 2018-02-02 15:14 /flume/test/FlumeData.1517584482646
       -rw-r--r--   1 root hdfs        394 2018-02-02 15:14 /flume/test/FlumeData.1517584482647
       -rw-r--r--   1 root hdfs        532 2018-02-02 15:14 /flume/test/FlumeData.1517584482648
       -rw-r--r--   1 root hdfs        707 2018-02-02 15:14 /flume/test/FlumeData.1517584482649
       -rw-r--r--   1 root hdfs        711 2018-02-02 15:14 /flume/test/FlumeData.1517584482650
       -rw-r--r--   1 root hdfs        712 2018-02-02 15:14 /flume/test/FlumeData.1517584482651
       -rw-r--r--   1 root hdfs        705 2018-02-02 15:14 /flume/test/FlumeData.1517584482652
       -rw-r--r--   1 root hdfs        713 2018-02-02 15:14 /flume/test/FlumeData.1517584482653
       -rw-r--r--   1 root hdfs        712 2018-02-02 15:14 /flume/test/FlumeData.1517584482654
       -rw-r--r--   1 root hdfs        705 2018-02-02 15:14 /flume/test/FlumeData.1517584482655
       -rw-r--r--   1 root hdfs        714 2018-02-02 15:14 /flume/test/FlumeData.1517584482656
       -rw-r--r--   1 root hdfs        711 2018-02-02 15:14 /flume/test/FlumeData.1517584482657
       
       
       
       [root@sandbox /]# hdfs dfs -cat /flume/test/FlumeData.1517584482703
       Nov 15 18:25:01 ephubudw3000 systemd: Started Session 2464 of user pcp.
       Nov 15 18:25:21 ephubudw3000 yum[12814]: Updated: python-qrcode-core-5.0.1-2.el7.noarch
       Nov 15 18:25:21 ephubudw3000 yum[12814]: Updated: libgpod-0.8.3-6.el7.x86_64
       Nov 15 18:25:21 ephubudw3000 yum[12814]: Updated: libsrtp-1.4.4-13.20101004cvs.el7.x86_64
       Nov 15 18:26:22 ephubudw3000 dbus[5893]: avc:  received setenforce notice (enforcing=0)
       Nov 15 18:26:22 ephubudw3000 dbus[5586]: avc:  received setenforce notice (enforcing=0)
       Nov 15 18:26:22 ephubudw3000 dbus[1021]: avc:  received setenforce notice (enforcing=0)
       Nov 15 18:26:22 ephubudw3000 dbus-daemon: dbus[1021]: avc:  received setenforce notice (enforcing=0)