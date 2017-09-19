package com.zxs.kafka;

import kafka.api.OffsetCommitRequest;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zengxs on 2017/1/12.
 */
public class TestKafka {

    private Producer p;

    @Before
    public void init() {
        p = new Producer();
        p.init();
    }

    @Test
    public void sendMsg() throws Exception {
        for (int i = 0; i < 100; i++) {
            p.send("test1", "this is message " + (i + 1));
        }
    }

    @Test
    public void receive() throws Exception {
        p.startConsumer("test1", 1);
        synchronized (p) {
            p.wait();
        }
    }


    @Test
    public void resetOffset() {
        List<PartitionInfo> partitionInfos = p.getConsumer().partitionsFor("test");
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("test", 0), new OffsetAndMetadata(0));
        offsets.put(new TopicPartition("test", 1), new OffsetAndMetadata(0));
        offsets.put(new TopicPartition("test", 2), new OffsetAndMetadata(0));
    }

}
