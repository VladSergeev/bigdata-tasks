package com.spark.streaming.kafka;


import com.spark.streaming.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof MonitoringRecord) {
            int numPartitions = cluster.availablePartitionsForTopic(topic).size();
            MonitoringRecord record = (MonitoringRecord) value;
            LOGGER.info("Parameter code: " + record.getParameterCode());
            return Integer.parseInt(record.getParameterCode()) % numPartitions;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }
}