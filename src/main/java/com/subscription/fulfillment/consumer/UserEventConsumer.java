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
public class UserEventConsumer {

    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "user-events", groupId = "fulfillment-service-group")
    public void handleUserEvent(String message) {
        EventEnvelope envelope;

        // Step 1: Deserialize (permanent failure if this fails - don't retry)
        try {
            envelope = objectMapper.readValue(message, EventEnvelope.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize user event (permanent failure): {}", e.getMessage());
            throw new IllegalArgumentException("Invalid event format", e);
        }

        // Step 2: Check if already processed (idempotency)
        if (idempotencyService.isAlreadyProcessed(envelope.getEventId())) {
            log.info("Duplicate event skipped: eventId={}", envelope.getEventId());
            return;
        }

        log.info("Processing user event: type={}, aggregateId={}, eventId={}",
                envelope.getEventType(), envelope.getAggregateId(), envelope.getEventId());
        log.debug("Event payload: {}", envelope.getPayload());

        // Step 3: Process event (transient failures will be retried)
        try {
            processEvent(envelope);

            // Step 4: Mark as processed ONLY after successful processing
            idempotencyService.markAsProcessed(envelope.getEventId());

        } catch (IllegalArgumentException | IllegalStateException e) {
            // Permanent failure - skip retry, mark as processed to avoid infinite retries
            log.error("Permanent failure processing user event: eventId={}, error={}",
                    envelope.getEventId(), e.getMessage());
            idempotencyService.markAsProcessed(envelope.getEventId());
            throw e; // Will go to DLQ
        } catch (Exception e) {
            // Transient failure - rethrow for retry
            log.error("Transient failure processing user event (will retry): eventId={}, error={}",
                    envelope.getEventId(), e.getMessage(), e);
            throw e; // Will be retried by DefaultErrorHandler
        }
    }

    private void processEvent(EventEnvelope envelope) {
        switch (envelope.getEventType()) {
            case "USER_REGISTERED" -> handleUserRegistered(envelope);
            case "USER_LOGIN" -> handleUserLogin(envelope);
            default -> log.warn("Unknown user event type: {}", envelope.getEventType());
        }
    }

    private void handleUserRegistered(EventEnvelope envelope) {
        log.info("Handling USER_REGISTERED for aggregateId={}", envelope.getAggregateId());
        log.debug("User registered payload: {}", envelope.getPayload());
    }

    private void handleUserLogin(EventEnvelope envelope) {
        log.info("Handling USER_LOGIN for aggregateId={}", envelope.getAggregateId());
        log.debug("User login payload: {}", envelope.getPayload());
    }
}
