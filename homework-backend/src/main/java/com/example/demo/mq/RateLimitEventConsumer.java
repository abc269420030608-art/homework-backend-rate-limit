package com.example.demo.mq;

import com.example.demo.repository.RateLimitViolationLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Example consumer: in production this is where audit, alert, or analytics work belongs. */
@Component
public class RateLimitEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RateLimitEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final RateLimitViolationLogRepository violationLogRepository;
    private final String nameServer;
    private final String topic;
    private DefaultMQPushConsumer consumer;

    public RateLimitEventConsumer(
            ObjectMapper objectMapper,
            RateLimitViolationLogRepository violationLogRepository,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.rate-limit-events}") String topic) {
        this.objectMapper = objectMapper;
        this.violationLogRepository = violationLogRepository;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            consumer = new DefaultMQPushConsumer("rate-limit-event-consumer");
            consumer.setNamesrvAddr(nameServer);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.subscribe(topic, "RATE_LIMIT_EXCEEDED");
            // DefaultMQPushConsumer overloads registerMessageListener for orderly and
            // concurrent listeners, so the listener type must be explicit here.
            MessageListenerConcurrently listener = (messages, context) -> {
                try {
                    for (var message : messages) {
                        RateLimitExceededEvent event = objectMapper.readValue(
                                new String(message.getBody(), StandardCharsets.UTF_8), RateLimitExceededEvent.class);
                        violationLogRepository.save(event.apiKey(), event.currentUsage(), event.occurredAt());
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception exception) {
                    log.error("Could not process rate-limit event batch; RocketMQ will retry it", exception);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            };
            consumer.registerMessageListener(listener);
            consumer.start();
            log.info("RocketMQ consumer subscribed to {}", topic);
        } catch (Exception exception) {
            consumer = null;
            log.warn("RocketMQ consumer is unavailable; event consumption will be skipped", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
