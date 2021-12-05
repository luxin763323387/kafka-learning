package com.cn.lx.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 创建5个kafkaProducer 发送消息
 * @author StevenLu
 * @date 2021/12/5 9:53 上午
 */
@Slf4j
@Component
public class KafkaSender {

    @Autowired
    private KafkaProperties kafkaProperties;

    private final KafkaProducer<String, String>[] kafkaProducers = new KafkaProducer[5];

    @PostConstruct
    public void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcksConfig());
        properties.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getRetriesConfig());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProperties.getBatchSizeConfig());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProperties.getLingerMsConfig());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProperties.getBufferMemoryConfig());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getKeySerializerClassConfig());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getValueSerializerClassConfig());

        // 为了提高性能，创建最多5个kafka broker连接
        for (int i = 0; i < 5; i++) {
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
            this.kafkaProducers[i] = kafkaProducer;
        }
    }

    public void send(String topic, String msg) {


        long startTime = System.currentTimeMillis();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        int index = ThreadLocalRandom.current().nextInt(5);
        KafkaProducer<String, String> kafkaProducer = this.kafkaProducers[index];
        kafkaProducer.send(record, (metadata, exception) -> {
            if (metadata != null) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                log.info("message(" + topic + ", " + msg + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            }
            if (exception != null) {
                log.error("**********************消息发送失败, 消息内容:{}", record.value());
//                emailUtils.sendMail(failedSubject, "you have msg which has been sent failed, " +
//                        "please process as soon as possible!!!");
            }
        });

    }


}
