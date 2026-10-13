package com.example.demo.service;

import com.example.demo.model.LimitCheckResult;
import com.example.demo.model.PageResponse;
import com.example.demo.model.RateLimitOutboxEvent;
import com.example.demo.model.RateLimitRule;
import com.example.demo.model.UsageResponse;
import com.example.demo.mq.OperationErrorEvent;
import com.example.demo.mq.OperationErrorEventPublisher;
import com.example.demo.mq.RateLimitEventPublisher;
import com.example.demo.mq.RateLimitExceededEvent;
import com.example.demo.mq.RateLimitOperationEvent;
import com.example.demo.mq.RateLimitOperationEventPublisher;
import com.example.demo.redis.RateLimitCounterRedis;
import com.example.demo.repository.RateLimitRepository;
import com.example.demo.repository.RateLimitOutboxRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RateLimitService {

    private final RateLimitRepository repository;
    private final RateLimitOutboxRepository outboxRepository;
    private final RateLimitCounterRedis counterStore;
    private final RateLimitEventPublisher eventPublisher;
    private final OperationErrorEventPublisher operationErrorEventPublisher;
    private final RateLimitOperationEventPublisher operationEventPublisher;

    public RateLimitService(
            RateLimitRepository repository,
            RateLimitOutboxRepository outboxRepository,
            RateLimitCounterRedis counterStore,
            RateLimitEventPublisher eventPublisher,
            OperationErrorEventPublisher operationErrorEventPublisher,
            RateLimitOperationEventPublisher operationEventPublisher) {
        this.repository = repository;
        this.outboxRepository = outboxRepository;
        this.counterStore = counterStore;
        this.eventPublisher = eventPublisher;
        this.operationErrorEventPublisher = operationErrorEventPublisher;
        this.operationEventPublisher = operationEventPublisher;
    }

    @Transactional
    public RateLimitRule createOrUpdate(RateLimitRule rule) {
        try {
            RateLimitRule existingRule = repository.findByApiKey(rule.apiKey()).orElse(null);
            if (sameRule(existingRule, rule)) {
                operationEventPublisher.publish(RateLimitOperationEvent.update(existingRule, existingRule));
                return existingRule;
            }
            RateLimitRule saved = repository.save(rule);
            outboxRepository.save(new RateLimitOutboxEvent(
                    UUID.randomUUID(), saved.apiKey(), saved.version(), saved.windowSeconds()));
            operationEventPublisher.publish(existingRule == null
                    ? RateLimitOperationEvent.create(saved)
                    : RateLimitOperationEvent.update(existingRule, saved));
            return saved;
        } catch (RuntimeException exception) {
            recordOperationError("CREATE_OR_UPDATE", rule.apiKey(), exception);
            throw exception;
        }
    }

    public LimitCheckResult check(String apiKey) {
        RateLimitRule rule = getRule(apiKey);
        long usage = counterStore.increment(apiKey, rule.windowSeconds(), rule.version());
        long ttl = counterStore.ttlSeconds(apiKey);
        boolean allowed = usage <= rule.limit();
        if (usage == (long) rule.limit() + 1) {
            eventPublisher.publish(RateLimitExceededEvent.from(apiKey, usage, rule.limit()));
        }
        return new LimitCheckResult(apiKey, allowed, usage, Math.max(0, rule.limit() - usage), ttl);
    }

    public UsageResponse usage(String apiKey) {
        RateLimitRule rule = getRule(apiKey);
        long usage = counterStore.currentUsage(apiKey);
        return new UsageResponse(apiKey, usage, Math.max(0, rule.limit() - usage), counterStore.ttlSeconds(apiKey));
    }

    public PageResponse<RateLimitRule> list(int page, int size) {
        int boundedSize = Math.min(size, 100);
        return new PageResponse<>(repository.findPage(boundedSize, page * boundedSize), page, boundedSize, repository.count());
    }

    public void delete(String apiKey) {
        try {
            RateLimitRule existingRule = repository.findByApiKey(apiKey)
                    .orElseThrow(() -> new RuleNotFoundException(apiKey));
            if (!repository.deleteByApiKey(apiKey)) {
                throw new RuleNotFoundException(apiKey);
            }
            counterStore.clear(apiKey);
            operationEventPublisher.publish(RateLimitOperationEvent.delete(existingRule));
        } catch (RuntimeException exception) {
            recordOperationError("DELETE", apiKey, exception);
            throw exception;
        }
    }

    private void recordOperationError(String operation, String apiKey, RuntimeException exception) {
        operationErrorEventPublisher.publish(OperationErrorEvent.from(operation, apiKey, exception));
    }

    private boolean sameRule(RateLimitRule existingRule, RateLimitRule rule) {
        return existingRule != null
                && existingRule.limit() == rule.limit()
                && existingRule.windowSeconds() == rule.windowSeconds();
    }

    private RateLimitRule getRule(String apiKey) {
        return repository.findByApiKey(apiKey).orElseThrow(() -> new RuleNotFoundException(apiKey));
    }

}
