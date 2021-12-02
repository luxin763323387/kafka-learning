package com.cn.lx.learning;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author StevenLu
 * @date 2021/11/27 8:11 下午
 */
public class ProducerSample {

    private final static String TOPIC_NAME = "sunday";

    public static void main(String[] args) throws Exception {
        // 异步
        //producerSend();

        // 异步阻塞
        producerSyncSend();
    }


    /*
        Producer异步发送带回调函数和Partition负载均衡
     */
    public static void producerSendWithCallbackAndPartition() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 - ProducerRecoder
        for (int i = 0; i < 50; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(
                            "partition : " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
                }
            });
        }

        // 所有的通道打开都需要关闭
        producer.close();
    }


    /**
     * 同步发送
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void producerSyncSend() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 - ProducerRecoder
        for (int i = 0; i < 50; i++) {
            String key = "key-" + i;
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, key, "value-" + i);

            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("=================================");
            System.out.println(key + "partition : " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
        }

        // 所有的通道打开都需要关闭
        producer.close();
    }

    /**
     * 异步发送
     */
    public static void producerSend() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 - ProducerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);

            producer.send(record);
        }

        // 所有的通道打开都需要关闭
        producer.close();
    }


}
