package com.subscription.fulfillment.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Queue (DLQ) Event Consumer.
 * 
 * Listens to all DLT (Dead Letter Topic) topics and logs failed events
 * that exhausted retry attempts.
 * 
 * Topics monitored:
 * - subscription-events.DLT
 * - product-events.DLT
 * - user-events.DLT
 * 
 * Future enhancements:
 * - Store in MongoDB dead_letter_events collection for manual review
 * - Send alerts to monitoring system (PagerDuty, Slack, etc.)
 * - Provide replay mechanism for recovered failures
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DlqEventConsumer {

    @KafkaListener(topics = {
            "subscription-events.DLT",
            "product-events.DLT",
            "user-events.DLT"
    }, groupId = "fulfillment-service-dlq-group")
    public void handleDlqEvent(ConsumerRecord<String, String> record) {
        log.error("""
                ========== DLQ EVENT RECEIVED ==========
                Topic: {}
                Partition: {}
                Offset: {}
                Key: {}
                Value: {}
                Timestamp: {}
                ========================================
                """,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                record.timestamp());

        // TODO: Store in MongoDB dead_letter_events collection for manual review
        // Example structure:
        // {
        // "originalTopic": record.topic().replace(".DLT", ""),
        // "partition": record.partition(),
        // "offset": record.offset(),
        // "key": record.key(),
        // "value": record.value(),
        // "timestamp": record.timestamp(),
        // "receivedAt": Instant.now(),
        // "status": "PENDING_REVIEW"
        // }

        // TODO: Send alert to monitoring system
        // alertService.sendCriticalAlert("DLQ Event Received", record.topic(),
        // record.value());
    }
}
