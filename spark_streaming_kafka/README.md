Realtime Anomaly Detection
==========================

This project is a prototype for real time anomaly detection.
It uses [Numenta's Hierarchical Temporal Memory](https://numenta.org/) -  a technology which emulates the work of the cortex.

Tasks:

Implement load data to Kafka and create Spark streaming for analyze data



### Kafka
Run Kafka.
Create kafka topics called "monitoring20" and "monitoringEnriched2", e.g.:
```
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic monitoring20
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic monitoringEnriched2
```    
Don't forget to expose port 6667 from docker of HDP 2.6 and add alias at hosts file:
```
127.0.0.1 sandbox.hortonworks.com
```
Because kafka broker will return to consumer and producer information to connect to sandbox.hortonworks.com.


### Zeppelin

Add following dependencies into spark2 interpreter(image zeppelin_config.png), click at profile and choose interpreter :
```
org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1
org.apache.kafka:kafka_2.11:0.10.2.1
org.apache.kafka:kafka-clients:0.10.2.1
org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1
```
Import anomaly-detector-kafka.json notebook into Zeppelin.

### Running
```
AnomalyDetector - is starting point for streaming
TopicGenerator - just read data from file to kafka topic   
```

## Requirements
    Devices need to be persistently assigned to kafka topic partitions.
    Only one instance of HTMNetwork(deviceID) per devece (deviceID = com.epam.bdcc.kafka.KafkaHelper.getKey(MonitoringRecord)) can be instantinated in spark streaming application.
    The order of the entries for the device must be saved.
    Max batch duration is 10 seconds! (Ideal is 2-3 seconds)
    All batches must be processed in the allotted interval, the first (HTMNetwork initialization) batch is an exception.
    KafkaProducer & ConsumerStrategy must be the following types (KafkaProducer<String, MonitoringRecord> & ConsumerStrategy<String, MonitoringRecord>).
    Restore from checkpoint is mandatory.
    
## Results
Run spark in zepplin and see graphs same as graphs.png.
1) Start streaming application (AnomalyDetector), to wait for incoming data.

2) Start visualizer prototype in zeppelin:
    - Start "Visualizer Section".
    - Start "Streaming Section".
    - Use "Stop Streaming Application" to stop streaming section.
