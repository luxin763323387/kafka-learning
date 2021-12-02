package com.cn.lx.producer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka发送回调
 * @author stevenlu
 */
@Slf4j
public class KakfaProducerCallBack implements Callback {

    private final long startTime;
    private final String key;
    private final String message;

    public KakfaProducerCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 当消息发送完成时，会调用这里的onCompletion来告知消息已经发送成功
     *
     * @param metadata
     * @param exception
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            //todo 异常业务处理
            exception.printStackTrace();
        }
    }
}
