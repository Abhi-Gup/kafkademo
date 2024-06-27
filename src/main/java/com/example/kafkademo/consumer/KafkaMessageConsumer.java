package com.example.kafkademo.consumer;


import com.example.kafkademo.model.Order;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaMessageConsumer {


    private static final Logger log = LoggerFactory.getLogger(KafkaMessageConsumer.class);

   // @KafkaListener(topics = "orders", groupId = "my-group-id")
    public void listen(String message) {
        log.info("Received message: " + message);
    }


   // @KafkaListener(topics = {"${json.message.topic.name}"}, containerFactory = "kafkaListenerJsonFactory", groupId = "my-group-id")
    public void consumeSuperHero(Order order) {
        log.info("**** -> Consumed Order :: {}", order);
    }
    
    

}
