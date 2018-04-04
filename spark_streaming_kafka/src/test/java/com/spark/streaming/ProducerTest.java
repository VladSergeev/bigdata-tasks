package com.spark.streaming;


import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.kafka.TopicGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProducerTest {
    private static final String FILE_PATH = "./data/simple_test.csv";
    private static final String TOPIC = "testTopic";
    private static final String BOOTSTRAP_SERVERS = "sandbox.hortonworks.com:6667";
    private KafkaConsumer consumer = null;

    private MonitoringRecord testMessage;

    @Before
    public void setUp() {

        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("group.id", "test_group");
        consumerProps.setProperty("client.id", "test_consumer");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "com.epam.bdcc.serde.KafkaJsonMonitoringRecordSerDe");
        consumerProps.setProperty("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        consumerProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumer = new KafkaConsumer(consumerProps);
        consumer.subscribe(asList(TOPIC));

        testMessage = new MonitoringRecord(
                "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(","));

    }

    @Test
    public void shouldSendDataToTopic() {
        TopicGenerator.produce(TOPIC, FILE_PATH);


        consumer.subscribe(Collections.singletonList(TOPIC));
        int retryCount = 3;
        while (retryCount >= 0) {
            ConsumerRecords records = consumer.poll(10000);
            Iterator<ConsumerRecord<String, MonitoringRecord>> iterator = records.iterator();
            if (records.isEmpty()) {
                retryCount--;
                continue;
            }

            if (iterator.hasNext()) {
                assertEquals(testMessage, iterator.next().value());
                retryCount=-666;
            }

            consumer.commitSync();
        }
        //That we read and not failed with reading
        assertThat(retryCount,is(-666));
    }
}
