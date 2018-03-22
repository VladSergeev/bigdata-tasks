package com.hadoop.task1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//https://github.com/tequalsme/hadoop-examples
public class MrUnitTest {
    MapDriver<Object, Text, IntWritable, Text> mapDriver;
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    MapReduceDriver<Object, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;
    WordMapper mapper = new WordMapper();
    LengthReducer reducer = new LengthReducer();

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void mustSplitAndFilterMaxLength() throws IOException {
        mapDriver
                .withInput(new LongWritable(1L), new Text("12345 123"))
                .withOutput(new IntWritable(5), new Text("12345"))
                .runTest();

    }

    @Test
    public void mustReduce() throws IOException {
        List<Text> shortWords = new ArrayList<>();
        shortWords.add(new Text("1234"));
        shortWords.add(new Text("4444"));
        reduceDriver.withInput(new IntWritable(4), shortWords);

        List<Text> longWords = new ArrayList<>();
        longWords.add(new Text("12345678"));

        reduceDriver.withInput(new IntWritable(8), longWords);
        reduceDriver.withOutput(new IntWritable(8), new Text("12345678"));
        reduceDriver.runTest();
    }

    @Test
    public void mustReduceSeveralWords() throws IOException {
        List<Text> shortWords = new ArrayList<>();
        shortWords.add(new Text("1234"));
        shortWords.add(new Text("4444"));
        reduceDriver.withInput(new IntWritable(4), shortWords);

        List<Text> longWords = new ArrayList<>();
        longWords.add(new Text("12345678"));
        longWords.add(new Text("12345679"));
        longWords.add(new Text("12345600"));

        reduceDriver.withInput(new IntWritable(8), longWords);
        reduceDriver.withOutput(new IntWritable(8), new Text("12345678"));
        reduceDriver.withOutput(new IntWritable(8), new Text("12345679"));
        reduceDriver.withOutput(new IntWritable(8), new Text("12345600"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("one cat dog 34432 very_long_word"));
        mapReduceDriver.addOutput( new IntWritable(14),new Text("very_long_word"));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceWithSeveralLongWords() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("one cat dog 34432 very_long_word word_very_long"));
        mapReduceDriver.addOutput( new IntWritable(14),new Text("very_long_word"));
        mapReduceDriver.addOutput( new IntWritable(14),new Text("word_very_long"));
        mapReduceDriver.runTest(false);
    }
}
