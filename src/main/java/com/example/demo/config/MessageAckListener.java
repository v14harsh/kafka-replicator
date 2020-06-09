package com.example.demo.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Arrays;

public class MessageAckListener implements AcknowledgingMessageListener<String, byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(MessageAckListener.class);

    private final KafkaTemplate<String, byte[]> template;
    private final String topic;

    public MessageAckListener(KafkaTemplate<String, byte[]> template, String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public void onMessage(ConsumerRecord<String, byte[]> data, Acknowledgment acknowledgment) {
        processConsumer(data);
        acknowledgment.acknowledge();
    }

    private void processConsumer(ConsumerRecord<String, byte[]> data) {
        logger.info("Thread: {}, hash: {}, topic: {}, data: {}", Thread.currentThread().getName(), Thread.currentThread().hashCode(), data.topic(), data);
//        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, data.partition(), data.timestamp(),
//                data.key(), data.value(), data.headers());
//        template.send(record);
    }

}
