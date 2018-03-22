package com.hadoop.task2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MrFlowTest {

    MapDriver<LongWritable, Text, Text, UserAgentInfo> mapDriver;

    ReduceDriver<Text, UserAgentInfo, Text, Text> reduceDriver;
    ReduceDriver<Text, UserAgentInfo, Text, UserAgentInfo> combineDriver;

    MapReduceDriver<LongWritable, Text, Text, UserAgentInfo, Text, Text> mapReduceDriver;


    @Before
    public void setUp() {
        LogMapper logMapper = new LogMapper();
        LogReducer logReducer = new LogReducer();
        LogCombiner logCombiner = new LogCombiner();
        mapDriver = MapDriver.newMapDriver(logMapper);
        reduceDriver = ReduceDriver.newReduceDriver(logReducer);
        combineDriver = ReduceDriver.newReduceDriver(logCombiner);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(logMapper, logReducer, logCombiner);
    }

    @Test
    public void mustMapData() throws IOException {
        String info = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
        mapDriver.withInput(new LongWritable(1), new Text(info));
        mapDriver.withOutput(new Text("ip1"), new UserAgentInfo(1L, 40028L));
        mapDriver.runTest();
    }

    @Test
    public void mustReturnNothing() throws IOException {
        String info = "Some wrong string";
        mapDriver.withInput(new LongWritable(1), new Text(info));
        mapDriver.runTest();
    }


    @Test
    public void mustReduceOne() throws IOException {
        List<UserAgentInfo> list = new ArrayList<>();
        list.add(new UserAgentInfo(2L, 10L));
        list.add(new UserAgentInfo(12L, 120L));
        reduceDriver.withInput(new Text("ip1"), list);
        reduceDriver.withOutput(new Text("ip1"), prepareReduceResponse(list));
        reduceDriver.runTest();
    }

    @Test
    public void mustReduceSeveral() throws IOException {
        List<UserAgentInfo> list = new ArrayList<>();
        list.add(new UserAgentInfo(2L, 10L));
        list.add(new UserAgentInfo(12L, 120L));
        reduceDriver.withInput(new Text("ip1"), list);
        reduceDriver.withOutput(new Text("ip1"), prepareReduceResponse(list));

        list = new ArrayList<>();
        list.add(new UserAgentInfo(1L, 10L));
        list.add(new UserAgentInfo(10L, 110L));

        reduceDriver.withInput(new Text("ip2"), list);
        reduceDriver.withOutput(new Text("ip2"), prepareReduceResponse(list));

        reduceDriver.runTest(false);
    }


    @Test
    public void mustCombine() throws IOException {
        List<UserAgentInfo> list = new ArrayList<>();
        list.add(new UserAgentInfo(2L, 10L));
        list.add(new UserAgentInfo(12L, 120L));
        combineDriver.withInput(new Text("ip1"), list);
        combineDriver.withOutput(new Text("ip1"), new UserAgentInfo(14L, 130L));
        combineDriver.runTest();
    }

    @Test
    public void mustIncrementOperaCounter() throws IOException {
        String info1 = "ip7 - - [24/Apr/2011:04:31:21 -0400] \"GET /sgi_indy/indy_monitor.jpg HTTP/1.1\" 200 12805 \"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\"";
        String info2 = "ip7 - - [24/Apr/2011:04:31:22 -0400] \"GET /sgi_indy/indy_inside.jpg HTTP/1.1\" 200 23285 \"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\"";
        mapDriver.withInput(new LongWritable(1), new Text(info1));
        mapDriver.withInput(new LongWritable(1), new Text(info2));
        mapDriver.withOutput(new Text("ip7"), new UserAgentInfo(1L, 12805L));
        mapDriver.withOutput(new Text("ip7"), new UserAgentInfo(1L, 23285L));
        mapDriver.runTest(false);
        assertEquals("Expected 1 counter increment", 2, mapDriver.getCounters().getGroup(LogMapper.BROWSER_COUNTER_GROUP).findCounter("opera").getValue());
    }


    @Test
    public void mustMapReduceValidData() throws IOException {
        String info1 = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
        String info2 = "Wrong line!";
        mapReduceDriver.withInput(new LongWritable(1), new Text(info1));
        mapReduceDriver.withInput(new LongWritable(2), new Text(info1));
        mapReduceDriver.withInput(new LongWritable(3), new Text(info2));
        mapReduceDriver.withOutput(new Text("ip1"), new Text("40028.0,80056"));
        mapReduceDriver.runTest(false);
    }

    @Test
    public void mustMapReduceOne() throws IOException {
        String info = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
        mapReduceDriver.withInput(new LongWritable(1), new Text(info));
        mapReduceDriver.withOutput(new Text("ip1"), new Text("40028.0,40028"));
        mapReduceDriver.runTest();
    }

    @Test
    public void mustMapReduceSeveral() throws IOException {
        String info1 = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
        String info2 = "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
        mapReduceDriver.withInput(new LongWritable(1), new Text(info1));
        mapReduceDriver.withInput(new LongWritable(2), new Text(info1));
        mapReduceDriver.withInput(new LongWritable(3), new Text(info2));
        mapReduceDriver.withOutput(new Text("ip1"), new Text("40028.0,80056"));
        mapReduceDriver.withOutput(new Text("ip2"), new Text("14917.0,14917"));
        mapReduceDriver.runTest(false);
    }


    private Text prepareReduceResponse(List<UserAgentInfo> list) {

        long total = 0;
        long count = 0;

        for (UserAgentInfo info : list) {
            total += info.getBytes().get();
            count += info.getCount().get();
        }

        BigDecimal bytes = new BigDecimal(total);
        BigDecimal countAll = new BigDecimal(count);
        bytes = bytes.setScale(1, BigDecimal.ROUND_HALF_UP);
        countAll = countAll.setScale(1, BigDecimal.ROUND_HALF_UP);
        BigDecimal avg = bytes.divide(countAll, 1, RoundingMode.HALF_UP);
        return new Text(avg + "," + total);
    }

}
