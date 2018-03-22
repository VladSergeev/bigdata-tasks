package com.hadoop.task2;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class UtilsTest {

    @Test
    public  void  mustParse(){
        String line = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
        Pattern r = Pattern.compile(LogMapper.LOG_REG_EXP);
        Matcher m = r.matcher(line);
        Assert.assertTrue(m.find());
        String ip = m.group(1);
        Assert.assertNotNull(ip);
        assertThat(ip,is("ip1"));
        String bytes = m.group(7);
        Assert.assertNotNull(bytes);
        assertThat(bytes,is("40028"));
        String userInfo = m.group(9);
        Assert.assertNotNull(userInfo);
    }

}
