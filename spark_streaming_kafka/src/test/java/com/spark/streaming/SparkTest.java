package com.spark.streaming;

import com.spark.streaming.kafka.TopicGenerator;
import com.spark.streaming.spark.SparkStreamingService;
import com.spark.streaming.utils.GlobalConstants;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SparkTest {

    private static final String BOOTSTRAP_SERVERS = "sandbox.hortonworks.com:6667";
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkTest.class);

    private String SOURCE_TOPIC="testInput";
    private String SINK_TOPIC="testOutput";

    private SparkStreamingService service;

    @Before
    public void setUP(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("test")
                .set(GlobalConstants.SPARK_STREAMING_BACKPRESSURE_ENABLED_CONFIG, "true")
                .set(GlobalConstants.SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, "true")
                .set(GlobalConstants.SPARK_KRYO_REGISTRATOR_CONFIG, "com.epam.bdcc.serde.SparkKryoHTMRegistrator")
                .set(GlobalConstants.SPARK_INTERNAL_SERIALIZER_CONFIG, "org.apache.spark.serializer.KryoSerializer");
        service=new SparkStreamingService(sparkConf);
    }

    @Test
    public void  createJSSC(){
        JavaStreamingContext streamingContext = service.createStreamingContext("./test", Duration.apply(1000));
        assertThat(streamingContext.ssc().checkpointDir().endsWith("/test"),is(true));
    }


    @Test
    public void monitoringProcessingTest() throws InterruptedException {
        TopicGenerator.produce(SOURCE_TOPIC,"./data/test.csv");
        JavaStreamingContext jssc = service.createStreamingContext("./test", Duration.apply(1000));
        service.kafkaMonitoringProcessing(jssc,SOURCE_TOPIC,SINK_TOPIC);
        jssc.start();
        jssc.awaitTerminationOrTimeout(30000);

        Thread.sleep(120000);


        Properties consumerProps = new Properties();
        consumerProps.setProperty("group.id", "test_group");
        consumerProps.setProperty("client.id", "test_consumer");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "com.epam.bdcc.serde.KafkaJsonMonitoringRecordSerDe");
        consumerProps.setProperty("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        consumerProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);

        KafkaConsumer consumer = new KafkaConsumer(consumerProps);
        consumer.subscribe(asList(SINK_TOPIC));

        int retryCount = 3;
        while (retryCount >= 0) {
            ConsumerRecords records = consumer.poll(1000);

            if (records.isEmpty()) {
                retryCount--;
                continue;
            }
            //for understanding
            records.records("testOutput").forEach(System.out::println);

            retryCount = -666;
            consumer.commitSync();
        }
        //That we read and not failed with reading
        assertThat(retryCount,is(-666));

    }



}
