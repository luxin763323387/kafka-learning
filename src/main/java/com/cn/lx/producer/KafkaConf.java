package com.cn.lx.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author StevenLu
 * @date 2021/12/1 10:58 下午
 */
@Configuration
public class KafkaConf {

    private final KafkaProperties kafkaProperties;

    public KafkaConf(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public Producer<?,?> kafkaProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcksConfig());
        properties.put(ProducerConfig.RETRIES_CONFIG,kafkaProperties.getRetriesConfig());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaProperties.getBatchSizeConfig());
        properties.put(ProducerConfig.LINGER_MS_CONFIG,kafkaProperties.getLingerMsConfig());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,kafkaProperties.getBufferMemoryConfig());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaProperties.getKeySerializerClassConfig());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,kafkaProperties.getValueSerializerClassConfig());

        // Producer的主对象
        Producer<String,String> producer = new KafkaProducer<>(properties);
        return producer;

    }
}
