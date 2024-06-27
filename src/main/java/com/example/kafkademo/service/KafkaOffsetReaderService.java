package com.example.kafkademo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import com.example.kafkademo.consumer.KafkaMessageConsumer;
import com.example.kafkademo.model.Order;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
public class KafkaOffsetReaderService {

    @Autowired
    private ConsumerFactory<String, Order> consumerFactory;

    @Value("${json.message.topic.name}")
    private String topicName;
    
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetReaderService.class);

    public List<Order> readMessagesFromOffset(long startOffset, long endOffset) {
    	 List<Order> jsonOrders = new ArrayList<>((int) endOffset - (int) startOffset + 1);
        KafkaConsumer<String, Order> kafkaConsumer = (KafkaConsumer)consumerFactory.createConsumer();
        TopicPartition topicPartition = new TopicPartition(topicName, 0); // Assuming partition 0
        kafkaConsumer.assign(Collections.singleton(topicPartition));

        kafkaConsumer.seek(topicPartition, startOffset);

        ConsumerRecords<String, Order> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, Order> record : records) {
            if (record.offset() > endOffset) {
                break;
            }
            processMessage(record);
            jsonOrders.add(record.value());
        }

        kafkaConsumer.close();
        
        return jsonOrders;
    }

    private void processMessage(ConsumerRecord<String, Order> record) {
        System.out.println("Received message: " + record.value() + ", offset: " + record.offset());
    }

	public List<Order> readMessagesFromOffset(int partition, long startOffset, long endOffset) {
		 List<Order> jsonOrders = new ArrayList<>((int) endOffset - (int) startOffset + 1);
	        KafkaConsumer<String, Order> kafkaConsumer = (KafkaConsumer)consumerFactory.createConsumer();
	        TopicPartition topicPartition = new TopicPartition(topicName, partition); // Assuming partition 0
	        kafkaConsumer.assign(Collections.singleton(topicPartition));

	        kafkaConsumer.seek(topicPartition, startOffset);

	        ConsumerRecords<String, Order> records = kafkaConsumer.poll(Duration.ofSeconds(5));
	        for (ConsumerRecord<String, Order> record : records) {
	            if (record.offset() > endOffset) {
	                break;
	            }
	            processMessage(record);
	            jsonOrders.add(record.value());
	        }

	        kafkaConsumer.close();
	        
	        return jsonOrders;
	}
}
