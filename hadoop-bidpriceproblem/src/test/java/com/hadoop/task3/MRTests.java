package com.hadoop.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MRTests {

    MapDriver<LongWritable, Text, IntWritable, CityEventCount> mapDriver;

    ReduceDriver<IntWritable, CityEventCount, Text, LongWritable> reduceDriver;
    ReduceDriver<IntWritable, CityEventCount, IntWritable, CityEventCount> combineDriver;

    MapReduceDriver<LongWritable, Text, IntWritable, CityEventCount, Text, LongWritable> mapReduceDriver;


    @Before
    public void setUp() {
        EventMapper bidMapper = new EventMapper();
        EventReducer bidReducer = new EventReducer();
        EventCombiner eventCombiner = new EventCombiner();
        mapDriver = MapDriver.newMapDriver(bidMapper);
        URL resource = getClass().getResource("/city.en.txt");
        File file = new File(resource.getFile());

        reduceDriver = ReduceDriver.newReduceDriver(bidReducer);
        reduceDriver.withCacheFile(file.toURI());
        combineDriver = ReduceDriver.newReduceDriver(eventCombiner);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(bidMapper, bidReducer, eventCombiner);
        mapReduceDriver.withCacheFile(file.toURI());
    }


    @Test
    public void mustMap() throws IOException {

        URL resource = getClass().getResource("/short_imp.20131019.txt");
        File file = new File(resource.getFile());
        List<String> data = Files.readAllLines(file.toPath());
        System.out.println("data size - " + data.size());
        for (int i = 0; i < data.size(); i++) {
            mapDriver.withInput(new LongWritable(i), new Text(data.get(i)));
        }

        mapDriver.withOutput(new IntWritable(226), new CityEventCount(new LongWritable(1), new LongWritable(277L)));
        mapDriver.withOutput(new IntWritable(234), new CityEventCount(new LongWritable(1), new LongWritable(277L)));
        mapDriver.withOutput(new IntWritable(226), new CityEventCount(new LongWritable(1), new LongWritable(277L)));
        mapDriver.withOutput(new IntWritable(234), new CityEventCount(new LongWritable(1), new LongWritable(277L)));
        mapDriver.withOutput(new IntWritable(133), new CityEventCount(new LongWritable(1), new LongWritable(150L)));
        mapDriver.runTest(false);
    }

    @Test
    public void mustCombine() throws IOException {

        List<CityEventCount> list = new ArrayList<>();
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(340L)));
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(160L)));

        combineDriver.withInput(new IntWritable(226), list);
        list = new ArrayList<>();
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(300L)));
        combineDriver.withInput(new IntWritable(227), list);


        combineDriver.withOutput(new IntWritable(226), new CityEventCount(new LongWritable(2), new LongWritable(500L)));
        combineDriver.withOutput(new IntWritable(227), new CityEventCount(new LongWritable(1), new LongWritable(300L)));
        combineDriver.runTest(false);
    }

    @Test
    public void mustReduce() throws IOException {

        List<CityEventCount> list = new ArrayList<>();
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(340L)));
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(160L)));

        reduceDriver.withInput(new IntWritable(226), list);
        list = new ArrayList<>();
        list.add(new CityEventCount(new LongWritable(1), new LongWritable(249L)));
        reduceDriver.withInput(new IntWritable(227), list);
        reduceDriver.withOutput(new Text("zhaoqing"), new LongWritable(2));
        reduceDriver.runTest(false);

    }


    @Test
    public void mustMapReduce() throws IOException {
        URL resource = getClass().getResource("/short_imp.20131019.txt");
        File file = new File(resource.getFile());
        List<String> data = Files.readAllLines(file.toPath());
        System.out.println("data size - " + data.size());
        for (int i = 0; i < data.size(); i++) {
            mapReduceDriver.withInput(new LongWritable(i), new Text(data.get(i)));
        }

        mapReduceDriver.withOutput(new Text("zhongshan"), new LongWritable(2L));
        mapReduceDriver.withOutput(new Text("zhaoqing"), new LongWritable(2L));
        mapReduceDriver.runTest(false);
    }

    @Test
    public void mustpartionCorrect() {
        CustomPartitioner partitioner = new CustomPartitioner();

        int partition = partitioner.getPartition(new IntWritable(321), new CityEventCount(new LongWritable(1), new LongWritable(160L)), 4);
        Assert.assertTrue(0 <= partition && partition < 4);
    }
}
