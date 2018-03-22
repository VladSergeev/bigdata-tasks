package com.hadoop.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class LogReducer extends Reducer<Text, UserAgentInfo, Text, Text> {

    private Text result = new Text();
    @Override
    protected void reduce(Text key, Iterable<UserAgentInfo> values, Context context) throws IOException, InterruptedException {

        long totalCount = 0;
        long totalBytes = 0;

        for (UserAgentInfo info : values) {
            totalCount = totalCount + info.getCount().get();
            totalBytes = totalBytes + info.getBytes().get();
        }
        BigDecimal avg = new BigDecimal(totalBytes);
        avg = avg.setScale(1, RoundingMode.HALF_UP);
        avg = avg.divide(new BigDecimal(totalCount), 1, RoundingMode.HALF_UP);
        result.set(avg + "," + totalBytes);
        context.write(key, result);
    }
}
