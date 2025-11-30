package com.subscription.fulfillment.service;

public interface IdempotencyService {

    /**
     * Check if event was already processed and mark it as processed if not.
     * Returns true if this is a NEW event (should be processed).
     * Returns false if this is a DUPLICATE event (should be skipped).
     *
     * Uses Redis SETNX (set if not exists) for atomic check-and-set.
     * 
     * @deprecated Use isAlreadyProcessed() + markAsProcessed() for retry support
     */
    @Deprecated
    boolean tryMarkAsProcessed(String eventId);

    /**
     * Check if an event has already been processed (for idempotency).
     * Does NOT mark as processed.
     * 
     * Use this before processing to allow retry without duplicate processing.
     */
    boolean isAlreadyProcessed(String eventId);

    /**
     * Mark an event as processed after successful processing.
     * Should be called AFTER business logic succeeds.
     * 
     * This allows retry - if processing fails, we can retry without duplicate side
     * effects.
     */
    void markAsProcessed(String eventId);
}
