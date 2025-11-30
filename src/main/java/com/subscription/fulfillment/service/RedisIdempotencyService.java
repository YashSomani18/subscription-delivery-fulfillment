package com.subscription.fulfillment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class RedisIdempotencyService implements IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:event:";
    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public boolean tryMarkAsProcessed(String eventId) {
        String key = KEY_PREFIX + eventId;
        String value = Instant.now().toString();

        Boolean wasSet = stringRedisTemplate.opsForValue().setIfAbsent(key, value, TTL);

        if (Boolean.TRUE.equals(wasSet)) {
            log.debug("Event marked as processed: eventId={}", eventId);
            return true;
        } else {
            log.info("Duplicate event detected, skipping: eventId={}", eventId);
            return false;
        }
    }

    @Override
    public boolean isAlreadyProcessed(String eventId) {
        String key = KEY_PREFIX + eventId;
        Boolean exists = stringRedisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    @Override
    public void markAsProcessed(String eventId) {
        String key = KEY_PREFIX + eventId;
        String value = Instant.now().toString();
        stringRedisTemplate.opsForValue().set(key, value, TTL);
        log.debug("Event marked as processed: eventId={}", eventId);
    }
}
