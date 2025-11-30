package com.subscription.fulfillment.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.subscription.shared.dto.event.EventEnvelope;
import com.subscription.fulfillment.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ProductEventConsumer {

    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "product-events", groupId = "fulfillment-service-group")
    public void handleProductEvent(String message) {
        EventEnvelope envelope;

        // Step 1: Deserialize (permanent failure if this fails - don't retry)
        try {
            envelope = objectMapper.readValue(message, EventEnvelope.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize product event (permanent failure): {}", e.getMessage());
            throw new IllegalArgumentException("Invalid event format", e);
        }

        // Step 2: Check if already processed (idempotency)
        if (idempotencyService.isAlreadyProcessed(envelope.getEventId())) {
            log.info("Duplicate event skipped: eventId={}", envelope.getEventId());
            return;
        }

        log.info("Processing product event: type={}, aggregateId={}, eventId={}",
                envelope.getEventType(), envelope.getAggregateId(), envelope.getEventId());
        log.debug("Event payload: {}", envelope.getPayload());

        // Step 3: Process event (transient failures will be retried)
        try {
            processEvent(envelope);

            // Step 4: Mark as processed ONLY after successful processing
            idempotencyService.markAsProcessed(envelope.getEventId());

        } catch (IllegalArgumentException | IllegalStateException e) {
            // Permanent failure - skip retry, mark as processed to avoid infinite retries
            log.error("Permanent failure processing product event: eventId={}, error={}",
                    envelope.getEventId(), e.getMessage());
            idempotencyService.markAsProcessed(envelope.getEventId());
            throw e; // Will go to DLQ
        } catch (Exception e) {
            // Transient failure - rethrow for retry
            log.error("Transient failure processing product event (will retry): eventId={}, error={}",
                    envelope.getEventId(), e.getMessage(), e);
            throw e; // Will be retried by DefaultErrorHandler
        }
    }

    private void processEvent(EventEnvelope envelope) {
        switch (envelope.getEventType()) {
            case "PRODUCT_CREATED" -> handleProductCreated(envelope);
            case "PRODUCT_UPDATED" -> handleProductUpdated(envelope);
            case "PRODUCT_OUT_OF_STOCK" -> handleProductOutOfStock(envelope);
            default -> log.warn("Unknown product event type: {}", envelope.getEventType());
        }
    }

    private void handleProductCreated(EventEnvelope envelope) {
        log.info("Handling PRODUCT_CREATED for aggregateId={}", envelope.getAggregateId());
        log.debug("Product created payload: {}", envelope.getPayload());
    }

    private void handleProductUpdated(EventEnvelope envelope) {
        log.info("Handling PRODUCT_UPDATED for aggregateId={}", envelope.getAggregateId());
        log.debug("Product updated payload: {}", envelope.getPayload());
    }

    private void handleProductOutOfStock(EventEnvelope envelope) {
        log.info("Handling PRODUCT_OUT_OF_STOCK for aggregateId={}", envelope.getAggregateId());
        log.debug("Product out of stock payload: {}", envelope.getPayload());
    }
}
