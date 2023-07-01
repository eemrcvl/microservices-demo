package com.microservices.demo.twitter.to.kafka.service.listener;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import com.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.v1.Status;
import twitter4j.v1.StatusAdapter;

@Slf4j
@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData,
                                      KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                      TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }

    @Override
    public void onStatus(Status status) {
        log.info("Twitter status with text: {}", status.getText());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromTwitterStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
