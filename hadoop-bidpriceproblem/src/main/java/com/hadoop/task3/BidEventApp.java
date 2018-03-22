package com.hadoop.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BidEventApp extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Need input and output files as parameters and city cache file!");
            return -1;
        }


        int numberOfReducers = 1;
        int cacheIndex = args.length - 1;
        try {
            numberOfReducers = Integer.parseInt(args[args.length - 1]);
            cacheIndex = args.length - 2;
        } catch (Exception e) {
            System.out.println("No number of reducers default is used!");
        }


        Configuration conf = getConf();
        Job job = new Job(conf, "BidEventApp");
        job.setJarByClass(BidEventApp.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(EventMapper.class);
        job.setCombinerClass(EventCombiner.class);
        job.setReducerClass(EventReducer.class);
        job.setPartitionerClass(CustomPartitioner.class);
        System.out.println("Number of reducers: " + numberOfReducers);
        job.setNumReduceTasks(numberOfReducers);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CityEventCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println("City cache file: " + args[cacheIndex]);
        job.addCacheFile(new URI(args[cacheIndex]));

        List<Path> paths = IntStream.range(0, cacheIndex - 1).mapToObj(x -> new Path(args[x])).collect(Collectors.toList());

        FileInputFormat.setInputPaths(job, paths.toArray(new Path[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[cacheIndex - 1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new BidEventApp(), args);
        System.exit(res);
    }
}
