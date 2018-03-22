package com.hadoop.task2;

import eu.bitwalker.useragentutils.BrowserType;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogMapper extends Mapper<LongWritable, Text, Text, UserAgentInfo> {
    private final Logger log = Logger.getLogger(LogMapper.class);
    public static final String BROWSER_COUNTER_GROUP = "Browser_counter";
    public static final String LOG_REG_EXP = "^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[(\\d{1,2}/\\w{1,3}/\\d{4}:\\d{1,2}:\\d{1,2}:\\d{1,2}\\s*[+\\-]\\d{4})\\]" +
            "\\s+\"(.+?)\"\\s+(\\d{1,3})\\s+(\\d*|-)\\s+\"(.+?)\"\\s+\"(.+?)\"";
    private Pattern pattern = null;

    private LongWritable one = new LongWritable(1);
    private LongWritable bytes = new LongWritable();
    private UserAgentInfo info = new UserAgentInfo();
    private Text ip = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pattern = Pattern.compile(LOG_REG_EXP);
        info.setCount(one);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        try {
            Matcher matcher = pattern.matcher(value.toString());
            if (matcher.find()) {
                ip.set(matcher.group(1));
                bytes.set(Long.parseLong(matcher.group(7)));
                info.setBytes(bytes);
                UserAgent userAgent = UserAgent.parseUserAgentString(matcher.group(9));
                if (userAgent.getBrowser() != null && isUserAgentCorrect(userAgent.getBrowser().getBrowserType()) && userAgent.getBrowser().getGroup() != null) {
                    Counter counter = context.getCounter(BROWSER_COUNTER_GROUP, userAgent.getBrowser().getGroup().getName().trim().toLowerCase());
                    counter.increment(1);
                }
                context.write(ip, info);
            }
        } catch (Exception e) {
            log.error("Can't parse line with offset: " + key.toString() +
                    " with value: " + value.toString(), e);
        }
    }

    private boolean isUserAgentCorrect(BrowserType browserType) {
        return browserType != null && (
                BrowserType.MOBILE_BROWSER.equals(browserType)
                        ||
                        BrowserType.WEB_BROWSER.equals(browserType)
                        ||
                        BrowserType.TEXT_BROWSER.equals(browserType));
    }

}
