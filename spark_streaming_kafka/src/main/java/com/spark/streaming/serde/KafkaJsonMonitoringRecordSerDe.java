package com.spark.streaming.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.streaming.htm.MonitoringRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);
    private ObjectMapper objectMapper;

    public KafkaJsonMonitoringRecordSerDe() {
        objectMapper = new ObjectMapper();
    }

    public KafkaJsonMonitoringRecordSerDe(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO : No need
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        byte[] result = null;
        try {
            result = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            LOGGER.error("Can't serialize message!", e);
        }
        return result;
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        MonitoringRecord record = null;
        try {
            record = objectMapper.readValue(data, MonitoringRecord.class);
        } catch (Exception e) {
            LOGGER.error("Can't deserialize message!", e);
        }
        return record;
    }

    @Override
    public void close() {
        //TODO : No need
    }
}