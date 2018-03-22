package com.hadoop.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogCombiner extends Reducer<Text, UserAgentInfo, Text, UserAgentInfo> {

    private UserAgentInfo info = new UserAgentInfo();
    private LongWritable countVal = new LongWritable();
    private LongWritable bytesVal = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<UserAgentInfo> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        long bytes = 0;

        for (UserAgentInfo agentInfo : values) {
            count += agentInfo.getCount().get();
            bytes += agentInfo.getBytes().get();
        }

        countVal.set(count);
        bytesVal.set(bytes);

        info.setBytes(bytesVal);
        info.setCount(countVal);

        context.write(key, info);
    }
}
