package com.example.demo.mq;

import com.example.demo.repository.RateLimitOperationLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Consumes successful rule-operation audits and writes them to MySQL. */
@Component
public class RateLimitOperationEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RateLimitOperationEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final RateLimitOperationLogRepository operationLogRepository;
    private final String nameServer;
    private final String topic;
    private DefaultMQPushConsumer consumer;

    public RateLimitOperationEventConsumer(
            ObjectMapper objectMapper,
            RateLimitOperationLogRepository operationLogRepository,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.rate-limit-operation-events}") String topic) {
        this.objectMapper = objectMapper;
        this.operationLogRepository = operationLogRepository;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            consumer = new DefaultMQPushConsumer("rate-limit-operation-event-consumer");
            consumer.setNamesrvAddr(nameServer);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.subscribe(topic, "RULE_OPERATION");
            MessageListenerConcurrently listener = (messages, context) -> {
                try {
                    for (var message : messages) {
                        RateLimitOperationEvent event = objectMapper.readValue(
                                new String(message.getBody(), StandardCharsets.UTF_8), RateLimitOperationEvent.class);
                        operationLogRepository.save(event);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception exception) {
                    log.error("Could not persist rate-limit operation event; RocketMQ will retry it", exception);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            };
            consumer.registerMessageListener(listener);
            consumer.start();
        } catch (Exception exception) {
            consumer = null;
            log.warn("RocketMQ operation-audit consumer is unavailable; audit events will not be persisted", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
