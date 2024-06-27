package com.example.kafkademo.producer;

import com.example.kafkademo.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaMessageProducer {
    @Value("${topic.name}")
    private String topic;

    @Value("${json.message.topic.name}")
    private String orders;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;


    private final KafkaTemplate<String, Order> kafkaJsonTemplate;

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String,Order> kafkaJsonTemplate ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaJsonTemplate = kafkaJsonTemplate;
    }

    public void sendMessage(String message) {

        kafkaTemplate.send(topic, message);
        log.info("Message : {} sent to topic : {}",message,topic);
    }

    public void sendJsonMessage(Order order)
    {
        //kafkaJsonTemplate.send(orders,order);
    	 kafkaJsonTemplate.send(orders,order.category(),order);
        log.info("Message : {} sent to topic : {}",order.toString(),orders);
    }
}
