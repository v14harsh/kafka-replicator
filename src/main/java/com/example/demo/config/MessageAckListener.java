package com.example.demo.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

public class MessageAckListener implements AcknowledgingMessageListener<String, byte[]> {

    private final KafkaTemplate<String, byte[]> template;

    public MessageAckListener(KafkaTemplate<String, byte[]> template) {
        this.template = template;
    }

    @Override
    public void onMessage(ConsumerRecord<String, byte[]> data, Acknowledgment acknowledgment) {
        processConsumer(data);
        acknowledgment.acknowledge();
    }

    private void processConsumer(ConsumerRecord<String, byte[]> data) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(data.topic(), data.partition(), data.timestamp(),
                data.key(), data.value(), data.headers());
        template.send(record);
    }

}
