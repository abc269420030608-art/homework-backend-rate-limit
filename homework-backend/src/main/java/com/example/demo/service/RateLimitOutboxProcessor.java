package com.example.demo.service;

import com.example.demo.model.RateLimitOutboxEvent;
import com.example.demo.redis.RateLimitCounterRedis;
import com.example.demo.repository.RateLimitOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Retries durable Redis rule updates after the MySQL transaction has committed. */
@Component
public class RateLimitOutboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(RateLimitOutboxProcessor.class);
    private static final int BATCH_SIZE = 100;

    private final RateLimitOutboxRepository outboxRepository;
    private final RateLimitCounterRedis counterStore;

    public RateLimitOutboxProcessor(
            RateLimitOutboxRepository outboxRepository, RateLimitCounterRedis counterStore) {
        this.outboxRepository = outboxRepository;
        this.counterStore = counterStore;
    }

    @Scheduled(fixedDelayString = "${rate-limit.outbox.fixed-delay-ms:1000}")
    public void processPendingEvents() {
        for (RateLimitOutboxEvent event : outboxRepository.findPending(BATCH_SIZE)) {
            try {
                counterStore.applyRule(event.apiKey(), event.ruleVersion(), event.windowSeconds());
                outboxRepository.markCompleted(event.id());
            } catch (RuntimeException exception) {
                outboxRepository.recordFailure(event.id(), exception);
                log.warn("Could not synchronize Redis rule for outbox event {}", event.id(), exception);
            }
        }
    }
}
