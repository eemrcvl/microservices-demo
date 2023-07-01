package com.microservices.demo.kafka.producer.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        log.info("Sending message: {} to topic: {}", message, topicName);

        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(
                topicName, key, message
        );

        kafkaResultFuture.whenComplete((result, exception) -> {
            if(exception == null) {
                log.info("Sent message {} with offset: {}", message, result.getRecordMetadata().offset());
            } else {
                log.error("Message {} was not sent due to error: {}", message, exception.getMessage());
            }
        });
    }
}
