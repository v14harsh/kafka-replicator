package com.example.demo.main;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

@Component
public class TransReceiver {

    @Value("${message.topic.name2}")
    private String topic2;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;


    @KafkaListener(topicPartitions = @TopicPartition(topic = "#{'${message.topic.name1}'}", partitions = { "0", "1" }))
//    @KafkaListener(topics = {"test-topic-1"})
    public void listenToParition( @Payload byte[] message,
                                  @Headers Map<String, Object> headers) {
        System.out.println(
                "Received Messasge: " + message
                        + "with headers: " + headers);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic2, message);
        headers.forEach((k, v) -> record.headers().add(k, getBytes(v)));
        kafkaTemplate.send(record);
    }

    private byte[] getBytes(Object v) {
        if (v instanceof String)
            return ((String) v).getBytes();
        else if (v instanceof Integer)
            return new byte[]{((Integer) v).byteValue()};
        else {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(v);
                return bos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

}
