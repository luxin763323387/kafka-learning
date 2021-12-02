package com.cn.lx.producer;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * kafka自定义配置
 *
 * @author StevenLu
 * @date 2021/12/1 10:53 下午
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "learning.kafka")
public class KafkaProperties {

    /**
     * 服务队地址
     */
    private String bootstrapServers;

    /**
     * 同步参数
     */
    private String acksConfig;

    /**
     * 重试参数
     */
    private String retriesConfig;

    /**
     * producer 将试图批处理消息记录，以减少请求 次数
     */
    private String batchSizeConfig;

    /**
     * 请求与发送之间的延迟
     */
    private String lingerMsConfig;

    /**
     * producer 可以用来缓存数据的内存大小
     */
    private String bufferMemoryConfig;

    /**
     * 消费组id
     */
    private String groupId;

    /**
     * 是否自动提交
     */
    private String enableAutoCommit;


    /**
     * 序列化类
     */
    private String keySerializerClassConfig;

    /**
     * 序列化类
     */
    private String valueSerializerClassConfig;
}
