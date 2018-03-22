package com.hadoop.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<IntWritable, CityEventCount> {
    @Override
    public int getPartition(IntWritable intWritable, CityEventCount cityEventCount, int numPartitions) {
        return (31 * intWritable.hashCode()) % numPartitions;
    }
}
