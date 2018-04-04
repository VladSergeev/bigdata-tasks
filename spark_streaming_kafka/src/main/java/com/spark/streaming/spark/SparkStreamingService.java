package com.spark.streaming.spark;

import com.spark.streaming.htm.HTMNetwork;
import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.htm.ResultState;
import com.spark.streaming.kafka.KafkaHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import scala.Tuple2;

import java.util.HashMap;


public class SparkStreamingService {

    private SparkConf sparkConf;

    public SparkStreamingService(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }


    public JavaStreamingContext createStreamingContext(String checkpointDir,
                                                       Duration batchDuration) {
        //create java streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);
        //setting checkpoint
        jssc.checkpoint(checkpointDir);
        return jssc;
    }

    public JavaPairDStream<String, MonitoringRecord> kafkaStream(String sourceTopic, JavaStreamingContext jssc) {
        //creating Kafka Direct Stream with consumer defined consumer strategy, contains proper settings
        //Kafka consumer record is not serializable
        return KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                KafkaHelper.createConsumerStrategy(sourceTopic))
                .mapToPair(consumerRecord -> new Tuple2<>(consumerRecord.key(), consumerRecord.value()));
    }

    public JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> mappingState(JavaPairDStream<String, MonitoringRecord> kafkaPairStream) {
        return kafkaPairStream.mapWithState(StateSpec.function(mappingFunc));
    }

    public void kafkaStreamProducing(JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> stateDStream,
                                     String sinkTopic) {
        stateDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(iterator -> {
                iterator.forEachRemaining(monitoringRecord -> {
                    try (KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer()) {
                        ProducerRecord<String, MonitoringRecord> record = new ProducerRecord<>(
                                sinkTopic, KafkaHelper.getKey(monitoringRecord), monitoringRecord);
                        producer.send(record);
                    }
                });
            });
        });


    }

    public void kafkaMonitoringProcessing(JavaStreamingContext jsscm, String sourceTopic, String sinkTopic) {
        JavaPairDStream<String, MonitoringRecord> kafkaStream = kafkaStream(sourceTopic, jsscm);
        JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> stateStream = mappingState(kafkaStream);
        kafkaStreamProducing(stateStream, sinkTopic);
    }

    private Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) -> {
                // case 0: timeout
                if (!recordOpt.isPresent())
                    return null;
                // either new or existing device
                if (!state.exists())
                    state.update(new HTMNetwork(deviceID));
                HTMNetwork htmNetwork = state.get();
                String stateDeviceID = htmNetwork.getId();

                if (!stateDeviceID.equals(deviceID))
                    throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
                MonitoringRecord record = recordOpt.get();

                // get the value of DT and Measurement and pass it to the HTM
                HashMap<String, Object> m = new HashMap<>();
                m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
                m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
                ResultState rs = htmNetwork.compute(m);
                record.setPrediction(rs.getPrediction());
                record.setError(rs.getError());
                record.setAnomaly(rs.getAnomaly());
                record.setPredictionNext(rs.getPredictionNext());

                return record;
            };
}
