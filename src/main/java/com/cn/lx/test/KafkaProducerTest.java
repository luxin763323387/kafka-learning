package com.cn.lx.test;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author StevenLu
 * @date 2021/12/1 11:56 下午
 */
@RestController
@RequestMapping(value = "/test/")
public class KafkaProducerTest {

    @Autowired
    private Producer<String, Object> producer;

    private final String topic = "sunday";
    private final String templateId = "templateId";

    @GetMapping("kafkaConfProducer")
    public void kafkaConfProducer() throws Exception {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, templateId, "测试测试");
//        Future<RecordMetadata> send = producer.send(record);
//        RecordMetadata recordMetadata = send.get();
//        System.out.println(templateId + "partition : "+recordMetadata.partition()+" , offset : "+recordMetadata.offset());

        producer.send(record, ((metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }
        }));
    }

}
