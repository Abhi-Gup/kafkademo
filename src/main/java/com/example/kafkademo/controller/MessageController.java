package com.example.kafkademo.controller;

import com.example.kafkademo.model.Order;
import com.example.kafkademo.producer.KafkaMessageProducer;
import com.example.kafkademo.service.KafkaOffsetReaderService;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    private final KafkaMessageProducer messageProducer;
    
    @Autowired
    private KafkaOffsetReaderService kafkaOffsetReaderService;

    public MessageController(KafkaMessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        messageProducer.sendMessage(message);
        return "Message sent: " + message;
    }

    @PostMapping("/publish")
    public String sendMessage(@RequestBody Order order) {
        messageProducer.sendJsonMessage(order);
        return "Message sent to Kafka topic";
    }
    
   
    @PostMapping("/setJsonOffsets")
    public ResponseEntity setJsonOffsets(@RequestParam long startOffset, @RequestParam long endOffset) {
        try {
            List<Order> messages = kafkaOffsetReaderService.readMessagesFromOffset(startOffset, endOffset);
            return ResponseEntity.ok("Messages consumed from offset " + startOffset + " to " + endOffset +""+ messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
    
    @PostMapping("/setJsonOffsetsWithPartition")
    public ResponseEntity setJsonOffsetsWithpartition(@RequestParam int partition,@RequestParam long startOffset, @RequestParam long endOffset) {
        try {
            List<Order> messages = kafkaOffsetReaderService.readMessagesFromOffset(partition,startOffset, endOffset);
            return ResponseEntity.ok("Messages consumed from offset " + startOffset + " to " + endOffset +""+ messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
    
}
