package com.subscription.fulfillment.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.subscription.fulfillment.service.IdempotencyService;
import com.subscription.shared.dto.event.EventEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SubscriptionEventConsumer {

    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "subscription-events", groupId = "fulfillment-service-group")
    public void handleSubscriptionEvent(String message) {
        EventEnvelope envelope;

        // Step 1: Deserialize (permanent failure if this fails - don't retry)
        try {
            envelope = objectMapper.readValue(message, EventEnvelope.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize subscription event (permanent failure): {}", e.getMessage());
            throw new IllegalArgumentException("Invalid event format", e);
        }

        // Step 2: Check if already processed (idempotency)
        if (idempotencyService.isAlreadyProcessed(envelope.getEventId())) {
            log.info("Duplicate event skipped: eventId={}", envelope.getEventId());
            return;
        }

        log.info("Processing subscription event: type={}, aggregateId={}, eventId={}",
                envelope.getEventType(), envelope.getAggregateId(), envelope.getEventId());
        log.debug("Event payload: {}", envelope.getPayload());

        // Step 3: Process event (transient failures will be retried)
        try {
            processEvent(envelope);

            // Step 4: Mark as processed ONLY after successful processing
            idempotencyService.markAsProcessed(envelope.getEventId());

        } catch (IllegalArgumentException | IllegalStateException e) {
            // Permanent failure - skip retry, mark as processed to avoid infinite retries
            log.error("Permanent failure processing subscription event: eventId={}, error={}",
                    envelope.getEventId(), e.getMessage());
            idempotencyService.markAsProcessed(envelope.getEventId());
            throw e; // Will go to DLQ
        } catch (Exception e) {
            // Transient failure - rethrow for retry
            log.error("Transient failure processing subscription event (will retry): eventId={}, error={}",
                    envelope.getEventId(), e.getMessage(), e);
            throw e; // Will be retried by DefaultErrorHandler
        }
    }

    private void processEvent(EventEnvelope envelope) {
        switch (envelope.getEventType()) {
            case "SUBSCRIPTION_CREATED" -> handleSubscriptionCreated(envelope);
            case "SUBSCRIPTION_UPDATED" -> handleSubscriptionUpdated(envelope);
            case "SUBSCRIPTION_CANCELLED" -> handleSubscriptionCancelled(envelope);
            default -> log.warn("Unknown subscription event type: {}", envelope.getEventType());
        }
    }

    private void handleSubscriptionCreated(EventEnvelope envelope) {
        log.info("Handling SUBSCRIPTION_CREATED for aggregateId={}", envelope.getAggregateId());
        log.debug("Subscription created payload: {}", envelope.getPayload());
    }

    private void handleSubscriptionUpdated(EventEnvelope envelope) {
        log.info("Handling SUBSCRIPTION_UPDATED for aggregateId={}", envelope.getAggregateId());
        log.debug("Subscription updated payload: {}", envelope.getPayload());
    }

    private void handleSubscriptionCancelled(EventEnvelope envelope) {
        log.info("Handling SUBSCRIPTION_CANCELLED for aggregateId={}", envelope.getAggregateId());
        log.debug("Subscription cancelled payload: {}", envelope.getPayload());
    }
}
