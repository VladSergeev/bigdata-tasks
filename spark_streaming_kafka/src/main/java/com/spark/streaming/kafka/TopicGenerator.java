package com.spark.streaming.kafka;

import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.utils.GlobalConstants;
import com.spark.streaming.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static com.spark.streaming.kafka.KafkaHelper.createProducer;
import static com.spark.streaming.kafka.KafkaHelper.getKey;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);


    public static void main(String[] args) {
        // load a properties file from class path, inside static method

        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            if (sampleFile == null || topicName == null) {
                throw new RuntimeException("Topic name or csv file path are not selected!");
            }
            produce(topicName, sampleFile);
        }
    }

    public static void produce(String topicName, String path) {
        try (KafkaProducer<String, MonitoringRecord> producer = createProducer()) {
            Files.lines(Paths.get(path), StandardCharsets.UTF_8)
                    .map(s -> new MonitoringRecord(s.split(",")))
                    .map((MonitoringRecord monitoringRecord) -> new ProducerRecord<>(topicName, getKey(monitoringRecord), monitoringRecord))
                    .forEach(producerRecord ->
                            producer.send(producerRecord,
                                    (recordMetadata, e) ->{
                                        if (e != null) {
                                            LOGGER.error("exception occur",e);
                                        } else {
                                            LOGGER.info(String.format("sent message to topic:%s partition:%s  offset:%s",
                                                    recordMetadata.topic(),
                                                    recordMetadata.partition(),
                                                    recordMetadata.offset()));
                                        }

                                    }



            ));
        } catch (Exception e) {
            LOGGER.error("Failed producing!", e);
        }
    }

}

