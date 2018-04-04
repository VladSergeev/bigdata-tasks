package com.hive.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class UDFTest {

    @Test
    public void mustParseUserAgent() {
        String line = "d503d08833987e6c792dc45ea1810800\t20131019161905104\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\tdf6e51cda096af59f06fe10e5d2285f6\tnull\t350639023\t300\t250\tOtherView\tNa\t89\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
        String[] columns = line.split("\t");
        String userAgentVal = columns[4];
        System.out.println(userAgentVal);
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentVal);
        Assert.assertTrue(userAgent.getOperatingSystem().getName().equalsIgnoreCase("Windows XP"));
        Assert.assertTrue(userAgent.getBrowser().getGroup().getName().equalsIgnoreCase("Chrome"));
        Assert.assertTrue(userAgent.getBrowser().getBrowserType().getName().equalsIgnoreCase("Browser"));
        Assert.assertTrue(userAgent.getOperatingSystem().getDeviceType().getName().equalsIgnoreCase("Computer"));
    }

    @Test
    public void mustGetNullWithNoData() {
        ParseUserAgent agent = new ParseUserAgent();
        Text value = null;
        Assert.assertNull(agent.evaluate(value));
    }

    @Test
    public void mustGetNullWithInvalidData() {
        ParseUserAgent agent = new ParseUserAgent();
        Assert.assertNull(agent.evaluate(new Text("That is stupid string")));
    }

    @Test
    public void mustGetParsedArray() {
        String line = "d503d08833987e6c792dc45ea1810800\t20131019161905104\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\tdf6e51cda096af59f06fe10e5d2285f6\tnull\t350639023\t300\t250\tOtherView\tNa\t89\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
        String[] columns = line.split("\t");
        String userAgentVal = columns[4];
        ParseUserAgent agent = new ParseUserAgent();
        ArrayList<String> data = agent.evaluate(new Text(userAgentVal));
        Assert.assertTrue(data.get(0).equalsIgnoreCase("Computer"));
        Assert.assertTrue(data.get(1).equalsIgnoreCase("Windows XP"));
        Assert.assertTrue(data.get(2).equalsIgnoreCase("Chrome"));
        Assert.assertTrue(data.get(3).equalsIgnoreCase("Browser"));
    }

}
