package com.hadoop.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class EventCombiner extends Reducer<IntWritable, CityEventCount, IntWritable, CityEventCount> {
    private final Logger log = Logger.getLogger(EventCombiner.class);
    private final CityEventCount eventCount = new CityEventCount();
    private final LongWritable count = new LongWritable();
    private final LongWritable price = new LongWritable();



    @Override
    protected void reduce(IntWritable key, Iterable<CityEventCount> values, Context context) throws IOException, InterruptedException {
        try {
            long countVal = 0L;
            long billPriceVal = 0L;
            for (CityEventCount eventCount : values) {
                countVal += eventCount.getCount().get();
                billPriceVal += eventCount.getPrice().get();
            }

            count.set(countVal);
            price.set(billPriceVal);
            eventCount.set(count, price);
            context.write(key, eventCount);
        } catch (Exception e) {
            log.error("Can't combine mapped data.", e);
            throw e;
        }
    }
}
