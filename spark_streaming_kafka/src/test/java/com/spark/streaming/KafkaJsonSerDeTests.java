package com.spark.streaming;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.serde.KafkaJsonMonitoringRecordSerDe;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class KafkaJsonSerDeTests {

    KafkaJsonMonitoringRecordSerDe serDe;
    String csvValue = "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12";

    @Before
    public void setUp() {
        serDe = new KafkaJsonMonitoringRecordSerDe();
    }

    @Test
    public void serializeNullObject() {
        byte[] data = serDe.serialize("topic", null);
        assertThat(data, is(not(nullValue())));
    }

    @Test
    public void serializeObject() {
        MonitoringRecord record = new MonitoringRecord(csvValue.split(","));
        byte[] data = serDe.serialize("topic", record);
        assertThat(data, is(not(nullValue())));
    }

    @Test
    public void serDeObject() {
        MonitoringRecord record = new MonitoringRecord(csvValue.split(","));
        byte[] data = serDe.serialize("topic", record);
        assertThat(data, is(not(nullValue())));

        MonitoringRecord deserialize = serDe.deserialize("topic", data);
        assertThat(deserialize, is(not(nullValue())));
        assertThat(deserialize, is(record));

    }

    @Test
    public void deserializeNullObject() {
        MonitoringRecord deserialize = serDe.deserialize("topic", null);
        assertThat(deserialize, is(nullValue()));
    }

    @Test
    public void withExceptionSerializeReturnNull() throws JsonProcessingException {
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        when(mapper.writeValueAsBytes(any())).thenThrow(new RuntimeException("Failed"));
        serDe = new KafkaJsonMonitoringRecordSerDe(mapper);

        MonitoringRecord record = new MonitoringRecord(csvValue.split(","));
        byte[] data = serDe.serialize("topic", record);
        assertThat(data, is(nullValue()));
    }

    @Test
    public void withExceptionDeserializeReturnNull() throws IOException {
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        when(mapper.readValue(Matchers.<byte[]>any(), Mockito.eq(MonitoringRecord.class))).thenThrow(new RuntimeException("Failed"));
        serDe = new KafkaJsonMonitoringRecordSerDe(mapper);

        MonitoringRecord record = new MonitoringRecord(csvValue.split(","));
        byte[] data = serDe.serialize("topic", record);
        assertThat(data, is(nullValue()));
    }
}
