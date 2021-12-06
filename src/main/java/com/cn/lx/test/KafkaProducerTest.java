package com.cn.lx.test;

import com.cn.lx.producer.KafkaSender;
import com.cn.lx.producer.KakfaProducerCallBack;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * @author StevenLu
 * @date 2021/12/1 11:56 下午
 */
@RestController
@RequestMapping(value = "/test/")
public class KafkaProducerTest {

    private final String topic = "sunday";
    private final String templateId = "templateId";

    private final Producer<String, Object> producer;

    public KafkaProducerTest(Producer<String, Object> producer) {
        this.producer = producer;
    }

    @Autowired
    private KafkaSender kafkaSender;

    @GetMapping("kafkaConfProducer")
    public void kafkaConfProducer() throws Exception {
        String value = "测试测试";
        String uuid = UUID.randomUUID().toString();
        long startTimes = System.currentTimeMillis();
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, templateId, uuid + value);
        producer.send(record, new KakfaProducerCallBack(startTimes, templateId, uuid + value));

//        简单写法
//        Future<RecordMetadata> send = producer.send(record);
//        RecordMetadata recordMetadata = send.get();
//        System.out.println(templateId + "partition : "+recordMetadata.partition()+" , offset : "+recordMetadata.offset());

    }

    @GetMapping("kafkaConfProducer1")
    public void kafkaConfProducer1() throws Exception {
        String value = "测试测试";
        String uuid = UUID.randomUUID().toString();
        long startTimes = System.currentTimeMillis();
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, templateId, uuid + value);
        kafkaSender.send(topic,uuid + value);

//        简单写法
//        Future<RecordMetadata> send = producer.send(record);
//        RecordMetadata recordMetadata = send.get();
//        System.out.println(templateId + "partition : "+recordMetadata.partition()+" , offset : "+recordMetadata.offset());

    }

}
