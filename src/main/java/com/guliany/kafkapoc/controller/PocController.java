package com.guliany.kafkapoc.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class PocController {

    @Value(value = "kafka.topic")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping
    public ResponseEntity<?> getControllerInfo() {
        log.info("Kafka POC Controller Accessed");
        return new ResponseEntity<>("Welcome to Kafka POC Controller", HttpStatus.OK);
    }

    @GetMapping("/{message}")
    public ResponseEntity<?> sendMessage(@PathVariable(name = "message") String message) {
        // kafkaTemplate.send(KafkaTopics.STARTER_TOPIC.getTopicName(), message); // basic
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate
                .send(topicName, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Sending Message to topic {} failed, due to {}",
                        topicName,
                        ex.getMessage()
                );
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            }
        });
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @KafkaListener(topics = "kafka-spring-poc", groupId = "foo")
    public void consume(String message) {
        log.info("Consumed message -> {}", message);
    }

}
