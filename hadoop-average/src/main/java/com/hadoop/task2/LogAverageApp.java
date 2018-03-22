package com.hadoop.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.stream.Stream;

public class LogAverageApp extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Stream.of(args).forEach(System.out::println);

        if (args.length < 2) {
            System.out.println("Need input and output files as parameters!");
            return -1;
        }


        boolean makeCompression = false;
        if (args.length > 2) {
            if ("seq".equals(args[2])) {
                makeCompression = true;
            }
        }

        Configuration conf = getConf();

        if (!makeCompression) {
            //to make CSV format
            conf.set("mapred.textoutputformat.separator", ",");
        }

        Job job = new Job(conf, "FindWord");
        job.setJarByClass(LogAverageApp.class);
        job.setJobName("LogAverageApp");


        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogCombiner.class);
        job.setReducerClass(LogReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserAgentInfo.class);
        //for sequence
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!makeCompression) {
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            //Block level is better than Record level, in most cases
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        }


        int code = job.waitForCompletion(true) ? 0 : 1;
        //just for simple visualization
        CounterGroup group = job.getCounters().getGroup(LogMapper.BROWSER_COUNTER_GROUP);
        //better to use logger
        group.spliterator().forEachRemaining(x ->
                System.out.println("Browser " + x.getName() + " used " + x.getValue())
        );

        return code;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LogAverageApp(), args);
        System.exit(res);
    }
}
