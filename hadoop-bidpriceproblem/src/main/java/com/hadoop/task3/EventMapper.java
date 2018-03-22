package com.hadoop.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class EventMapper extends Mapper<LongWritable, Text, IntWritable, CityEventCount> {
    private final Logger log = Logger.getLogger(EventMapper.class);


    private final CityEventCount eventCount = new CityEventCount();
    private final LongWritable one = new LongWritable(1L);
    private final LongWritable price = new LongWritable();
    private final IntWritable cityId = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] split = value.toString().trim().split("\t");
            Integer cityIdValue = Integer.parseInt(split[7]);


            Long bidPrice = Long.parseLong(split[19]);

            cityId.set(cityIdValue);

            price.set(bidPrice);

            eventCount.set(one, price);

            context.write(cityId, eventCount);

        } catch (Exception e) {
            log.error("Can't read line " + key + " value is " + value, e);
            throw new RuntimeException(e);
        }
    }


}
