package com.cn.lx.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者例子
 *
 * @author StevenLu
 * @date 2021/12/2 11:18 下午
 */
public class ConsumerSample {

    private final static String TOPIC_NAME = "sunday";

    public static void main(String[] args) {
        //helloworld();
        commitedOffset();
    }

    /*
       手动提交offset,并且手动控制partition
    */
    private static void commitedOffsetWithPartition() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        // 消费订阅哪一个Topic或者几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for(TopicPartition partition : records.partitions()){
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());

                }
                long lastOffset = pRecord.get(pRecord.size() -1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition,new OffsetAndMetadata(lastOffset+1));
                // 提交offset
                consumer.commitSync(offset);
                System.out.println("=============partition - "+ partition +" end================");
            }
        }
    }


    private static void commitedOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                //成功入库
                System.out.printf("partition = %d , offset = %d , key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                System.out.println("=================");
                //失败回滚，不要提交offset

            }
            //如果全部成功, 手动提交offset
            consumer.commitAsync();
        }
    }

    /**
     * 自动提交
     * 工作中不推荐
     */
    private static void helloworld() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("=================");
                System.out.printf("partition = %d , offset = %d , key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                System.out.println("=================");
            }
        }
    }
}
