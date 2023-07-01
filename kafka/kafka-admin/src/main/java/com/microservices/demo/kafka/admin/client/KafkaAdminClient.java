package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class KafkaAdminClient {

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,
                            AdminClient adminClient,
                            RetryTemplate retryTemplate,
                            WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch(Throwable t) {
            throw new KafkaClientException("Reached max number of retry attempts for creating kafka topics!", t);
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating {} topics, attempt: {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic ->
            new NewTopic(
                    topic.trim(),
                    kafkaConfigData.getNumOfPartitions(),
                    kafkaConfigData.getReplicationFactor()
            )).toList();
        return adminClient.createTopics(kafkaTopics);
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topicListings;
        try {
            topicListings = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry attempts for reading kafka topics!", t);
        }

        return topicListings;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Reading kafka topic: {}, attempt: {}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();
        if(topicListings != null) {
            topicListings.forEach(topicListing -> log.info("Topic with name: {}", topicListing.name()));
        }
        return topicListings;
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            ResponseEntity<Void> response = webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            Assert.notNull(response, "Resppnse cannot be null");
            return HttpStatus.valueOf(response.getStatusCode().value());
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    public void checkTopicsCreated() {
        Collection<TopicListing> topicListings = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        for(String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while(!isTopicCreated(topicListings, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topicListings = getTopics();
            }
        }
    }

    private void checkMaxRetry(int retryCount, Integer maxRetry) {
        if(retryCount > maxRetry) {
            throw new KafkaClientException("Reached max number of retry attempts for reading kafka topics!");
        }
    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping for waiting topic creation", e);
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topicListings, String topic) {
        if(topicListings == null)
            return false;

        return topicListings.stream().anyMatch(topicListing -> topicListing.name().equals(topic));
    }
}
