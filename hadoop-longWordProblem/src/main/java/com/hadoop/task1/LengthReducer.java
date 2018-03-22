package com.hadoop.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Reducer that filters key/values for these where key is maximum.
 */
public class LengthReducer extends Reducer<IntWritable, Text, IntWritable, Text> {


    private Integer maxKey = Integer.MIN_VALUE;
    private Iterable<Text> words = null;
    private IntWritable wordLength = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.get() > maxKey) {
            maxKey = key.get();
            words = values;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        if (words != null) {
            words.forEach(x -> {
                try {
                    wordLength.set(maxKey);
                    context.write(wordLength, x);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException("Can't save data", e);
                }

            });

        }
    }
}
