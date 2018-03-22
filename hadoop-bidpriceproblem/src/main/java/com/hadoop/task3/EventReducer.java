package com.hadoop.task3;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class EventReducer extends Reducer<IntWritable, CityEventCount, Text, LongWritable> {
    private final static long BILL_PRICE_THRESHOLD = 250L;
    private final static String UNKNOWN_CITY = "UNKNOWN";
    private final Map<Integer, String> cache = new HashMap<>();
    private final Logger log = Logger.getLogger(EventReducer.class);
    private final Text city = new Text();
    private final LongWritable count = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Path[] files = context.getLocalCacheFiles();
        if (files.length != 1) {
            throw new RuntimeException("We have support only for city cache!");
        }
        Path file = files[0];
        FileSystem fileSystem = FileSystem.getLocal(context.getConfiguration());

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(file)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.trim().split("[\\s\\t]");
                if (split.length != 2) {
                    throw new RuntimeException("Strange line occurs: " + line);
                }
                Integer id = Integer.parseInt(split[0]);
                cache.put(id, split[1]);
            }

        } catch (Exception e) {
            log.error("Failed to load cache", e);
            throw e;
        }

    }

    @Override
    protected void reduce(IntWritable key, Iterable<CityEventCount> values, Context context) throws IOException, InterruptedException {

        try {
            long countVal = 0L;
            long billPriceVal = 0L;
            for (CityEventCount eventCount : values) {
                countVal += eventCount.getCount().get();
                billPriceVal += eventCount.getPrice().get();
            }

            if (billPriceVal > BILL_PRICE_THRESHOLD) {
                count.set(countVal);
                String cityVal = cache.getOrDefault(key.get(), UNKNOWN_CITY);
                city.set(cityVal.trim());
                context.write(city, count);
            }
        } catch (Exception e) {
            log.error("Can't combine mapped data.", e);
            throw e;
        }


    }
}
