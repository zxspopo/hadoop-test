package com.yonyou.zxs.mr.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created by zengxs on 2016/12/14.
 */
public class ConfigurationTest {

    @Test
    public void testConf() {
        Configuration conf = new Configuration();
        conf.addResource(getClass().getClassLoader().getResourceAsStream("configuration-1.xml"));
        // for (Map.Entry<String, String> entry : conf) {
        // System.out.println(entry.getKey() + ":" + entry.getValue());
        // }
        Assert.assertEquals(conf.get("color"), "yellow");
        Assert.assertEquals(conf.getInt("size", 0), 10);
        Assert.assertEquals(conf.get("breath", "wide"), "wide");
        Assert.assertEquals(conf.get("size-weight"), "10,heavy");
    }

    /**
     * 资源合并和资源覆盖
     * final 属性不能覆盖
     */
    @Test
    public void testOverride() {
        Configuration conf = new Configuration();
        conf.addResource(getClass().getClassLoader().getResourceAsStream("configuration-1.xml"));
        conf.addResource(getClass().getClassLoader().getResourceAsStream("configuration-2.xml"));
        Assert.assertEquals(conf.getInt("size", 0), 12);
        Assert.assertEquals(conf.get("weight"), "heavy");
    }




}
