package com.zxs.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Created by zengxs on 2017/1/11.
 */
public class Producer {

    private KafkaProducer producer;

    private KafkaConsumer<String, String> consumer;

    public void init() {

        Map<String, Object> config = new HashMap<String, Object>();
        // kafka服务器地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // kafka消息序列化类 即将传入对象序列化为字节数组
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // kafka消息key序列化类 若传入key的值，则根据该key的值进行hash散列计算出在哪个partition上
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024 * 5);
        // 往kafka服务器提交消息间隔时间，0则立即提交不等待
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // 消费者配置文件
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-nn-02:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "false");
        // props.put("auto.commit.interval.ms", "1000");
        props.put("dual.commit.enabled", "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");


        producer = new KafkaProducer(config);
        consumer = new KafkaConsumer<>(props);

    }

    /**
     * 发送消息，发送的对象必须是可序列化的
     */
    public Future<RecordMetadata> send(String topic, Serializable value) throws Exception {
        try {
            // 将对象序列化称字节码
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, Serializable>(topic, value));
            return future;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 启动一个消费程序
     * 
     * @param topic 要消费的topic名称
     * @param threadCount 消费线程数，该值应小于等于partition个数，多了也没用
     */
    public <T extends Serializable> void startConsumer(String topic, int threadCount) throws Exception {
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> partitionSet = consumer.assignment();
        System.out.println(partitionSet);
        Executors.newFixedThreadPool(1).submit(new Thread() {
            public void run() {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
                                record.value());
                    }
                    consumer.commitAsync();
                }
            }
        });
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer producer) {
        this.producer = producer;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }
}
