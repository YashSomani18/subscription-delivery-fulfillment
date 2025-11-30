package com.subscription.fulfillment.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.ExponentialBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Kafka configuration for resilient event consumption.
 * 
 * Features:
 * - Exponential backoff retry (3 attempts: 1s, 2s, 4s)
 * - Dead Letter Queue (DLQ) for failed messages after retries
 * - Non-retryable exceptions (permanent failures)
 * - Retry listeners for monitoring
 */
@Configuration
@Slf4j
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // DLQ recoverer - sends failed messages to {topic}.DLT (Dead Letter Topic)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    String dlqTopic = record.topic() + ".DLT";
                    log.error("Sending message to DLQ: topic={}, partition={}, offset={}, dlqTopic={}",
                            record.topic(), record.partition(), record.offset(), dlqTopic);
                    return new org.apache.kafka.common.TopicPartition(dlqTopic, record.partition());
                });

        // Exponential backoff: 1s, 2s, 4s (3 total retries)
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxElapsedTime(7000L); // Max 7 seconds total for all retries

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Don't retry on these exceptions (permanent failures)
        errorHandler.addNotRetryableExceptions(
                JsonProcessingException.class,
                NullPointerException.class,
                IllegalArgumentException.class,
                IllegalStateException.class);

        // Log retry attempts for monitoring
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Retry attempt {} for topic={}, partition={}, offset={}, exception={}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage());
        });

        // Log when moving to DLQ
        errorHandler.setCommitRecovered(true);

        return errorHandler;
    }
}
