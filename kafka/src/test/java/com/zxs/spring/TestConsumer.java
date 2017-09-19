package com.zxs.spring;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by zengxs on 2017/8/28.
 */
@ContextConfiguration(locations = "/spring-consumer.xml")
public class TestConsumer extends AbstractJUnit4SpringContextTests {

    private Object lock = new Object();

    @Test
    public void testConsumer() throws InterruptedException {
        synchronized (lock) {
            lock.wait();
        }
    }
}
