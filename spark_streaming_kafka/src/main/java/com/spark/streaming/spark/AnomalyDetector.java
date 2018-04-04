package com.spark.streaming.spark;

import com.spark.streaming.utils.GlobalConstants;
import com.spark.streaming.utils.PropertiesLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Properties;

public class AnomalyDetector implements GlobalConstants {
    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/

    private final static String TRUE = "true";

    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String sourceTopic = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String sinkTopic = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));

            SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
                    .set(GlobalConstants.SPARK_STREAMING_BACKPRESSURE_ENABLED_CONFIG, TRUE)
                    .set(GlobalConstants.SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, TRUE)
                    .set(GlobalConstants.SPARK_KRYO_REGISTRATOR_CONFIG, "com.epam.bdcc.serde.SparkKryoHTMRegistrator")
                    .set(GlobalConstants.SPARK_INTERNAL_SERIALIZER_CONFIG, "org.apache.spark.serializer.KryoSerializer");

            SparkStreamingService service= new SparkStreamingService(sparkConf);

            JavaStreamingContext jssc = service.createStreamingContext(checkpointDir, batchDuration);
            service.kafkaMonitoringProcessing(jssc,sourceTopic,sinkTopic);
            jssc.start();
            jssc.awaitTermination();
        }
    }

}