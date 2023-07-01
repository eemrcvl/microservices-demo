package com.microservices.demo.twitter.to.kafka.service.transformer;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.v1.Status;

import java.time.ZoneId;
import java.time.ZonedDateTime;

@Component
public class TwitterStatusToAvroTransformer {

    public TwitterAvroModel getTwitterAvroModelFromTwitterStatus(Status status) {
        return TwitterAvroModel.newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(ZonedDateTime.of(status.getCreatedAt(), ZoneId.systemDefault()).toInstant().toEpochMilli())
                .build();
    }
}
