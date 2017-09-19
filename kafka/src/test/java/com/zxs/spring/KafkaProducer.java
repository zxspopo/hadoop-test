package com.zxs.spring;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by zengxs on 2017/8/28.
 */
@ContextConfiguration(locations = "/spring-producer.xml")
public class KafkaProducer extends AbstractJUnit4SpringContextTests {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Test
    public void testSendMessage() {
        for (int i = 0; i < 1000; i++) {
            kafkaTemplate.send("test", "this is message <" + i + ">");
        }
    }

}
