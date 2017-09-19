package com.zxs.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

/**
 * Created by zengxs on 2017/8/28.
 */
public class KafkaConsumer implements MessageListener<Integer, String> {


    @Override
    public void onMessage(ConsumerRecord<Integer, String> integerStringConsumerRecord) {
        System.out.println(integerStringConsumerRecord);
    }
}
