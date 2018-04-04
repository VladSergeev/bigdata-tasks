package com.hive.udf;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;


public class ParseUserAgent extends UDF {

    // Main thing is to return ArrayList of Strings, otherwise you will get Semantic exception.
    public ArrayList<String> evaluate(Text value) {
        if (value == null) {
            return null;
        }
        return parse(value.toString());
    }

    private ArrayList<String> parse(String value) {
        UserAgent userAgent = UserAgent.parseUserAgentString(value);
        boolean successParse = userAgent.getOperatingSystem() != null &&
                !userAgent.getOperatingSystem().equals(OperatingSystem.UNKNOWN) &&
                userAgent.getOperatingSystem().getDeviceType() != null &&
                userAgent.getBrowser() != null &&
                !userAgent.getBrowser().equals(Browser.UNKNOWN) &&
                userAgent.getBrowser().getGroup() != null;
        if (!successParse) {
            return null;
        }
        ArrayList<String> result = new ArrayList<>();
        result.add(userAgent.getOperatingSystem().getDeviceType().getName());
        result.add(userAgent.getOperatingSystem().getName());
        result.add(userAgent.getBrowser().getGroup().getName());
        result.add(userAgent.getBrowser().getBrowserType().getName());
        return result;

    }
}
