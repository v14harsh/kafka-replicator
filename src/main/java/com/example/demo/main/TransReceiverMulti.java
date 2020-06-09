package com.example.demo.main;

import com.example.demo.config.ConsumerUtil;
import com.example.demo.config.MessageAckListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class TransReceiverMulti {

    @Value("${message.topic.name2}")
    private String topic2;

    @Value("${message.topic.name1}")
    private String topic1;

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @PostConstruct
    public void construct() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapAddress);
        props.put("group.id", "foo");
        props.put("enable.auto.commit", false);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // inject the processor to the kafka message listener
        MessageAckListener customAckMessageListener = new MessageAckListener(kafkaTemplate, topic2);

        // start the consumer
        ConsumerUtil.startOrCreateConsumers(topic1, customAckMessageListener, 5, props);

        // start the consumer
        ConsumerUtil.startOrCreateConsumers(topic2, customAckMessageListener, 5, props);
    }

}
