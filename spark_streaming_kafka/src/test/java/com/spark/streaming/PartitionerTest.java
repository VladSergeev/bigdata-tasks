package com.spark.streaming;

import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.kafka.MonitoringRecordPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class PartitionerTest {

    private MonitoringRecordPartitioner partitioner = new MonitoringRecordPartitioner();


    @Test
    public void withMonitoringRecordCustomPartitioning() {


        PartitionInfo fakePartitionInfo = new PartitionInfo("test", 1, Node.noNode(), null, null);
        Cluster mock = Mockito.mock(Cluster.class);
        when(mock.availablePartitionsForTopic(Matchers.eq("test"))).thenReturn(asList(fakePartitionInfo, fakePartitionInfo, fakePartitionInfo));
        MonitoringRecord record = new MonitoringRecord(
                "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(","));

        int partition = partitioner.partition("test", null, null, record, null, mock);

        int code = Integer.parseInt(record.getParameterCode());
        assertThat(partition, is(code % 3));
    }
}
