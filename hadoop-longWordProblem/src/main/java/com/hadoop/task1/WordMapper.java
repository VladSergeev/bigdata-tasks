package com.hadoop.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Mapper that splits words and filters for words with longest length
 */
public class WordMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static final String SPLIT_REGEX = " ";
    private Text word = new Text();
    private IntWritable wordLength = new IntWritable();

    //Prefiltering of words
    private Integer maxLength = Integer.MIN_VALUE;
    private Set<String> maxSizeWords = new HashSet<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(SPLIT_REGEX);

        for (String word : words) {
            if (word.length() > maxLength) {
                maxSizeWords.clear();
                maxLength = word.length();
                maxSizeWords.add(word);
            } else if (word.length() == maxLength) {
                maxSizeWords.add(word);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String val : maxSizeWords) {
            word.set(val);
            wordLength.set(val.length());
            context.write(wordLength, word);
        }
    }
}
