package com.example.demo.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.demo.model.RateLimitRule;
import com.example.demo.mq.OperationErrorEventPublisher;
import com.example.demo.mq.RateLimitEventPublisher;
import com.example.demo.mq.RateLimitOperationEventPublisher;
import com.example.demo.redis.RateLimitCounterRedis;
import com.example.demo.repository.RateLimitRepository;
import com.example.demo.repository.RateLimitOutboxRepository;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RateLimitServiceTest {

    private final RateLimitRepository repository = mock(RateLimitRepository.class);
    private final RateLimitCounterRedis counterStore = mock(RateLimitCounterRedis.class);
    private final RateLimitService service = new RateLimitService(
            repository,
            mock(RateLimitOutboxRepository.class),
            counterStore,
            mock(RateLimitEventPublisher.class),
            mock(OperationErrorEventPublisher.class),
            mock(RateLimitOperationEventPublisher.class));

    @Test
    void updatingWindowAdjustsExpiryWithoutClearingTheCounter() {
        RateLimitRule existing = new RateLimitRule("client-a", 10, 60);
        RateLimitRule updated = new RateLimitRule("client-a", 10, 120);
        when(repository.findByApiKey("client-a")).thenReturn(Optional.of(existing));
        when(repository.save(updated)).thenReturn(updated);

        service.createOrUpdate(updated);

        verify(counterStore, never()).applyRule("client-a", 0, 120);
        verify(counterStore, never()).clear("client-a");
    }

    @Test
    void updatingOnlyLimitKeepsCounterAndExpiry() {
        RateLimitRule existing = new RateLimitRule("client-a", 10, 60);
        RateLimitRule updated = new RateLimitRule("client-a", 20, 60);
        when(repository.findByApiKey("client-a")).thenReturn(Optional.of(existing));
        when(repository.save(updated)).thenReturn(updated);

        service.createOrUpdate(updated);

        verify(counterStore, never()).applyRule("client-a", 0, 60);
        verify(counterStore, never()).clear("client-a");
    }
}
