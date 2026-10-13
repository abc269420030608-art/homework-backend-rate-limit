package com.example.demo.mq;

import com.example.demo.repository.OperationErrorLogRepository;
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

/** Consumes operation failures and persists them in the error-log table. */
@Component
public class OperationErrorEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OperationErrorEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final OperationErrorLogRepository errorLogRepository;
    private final String nameServer;
    private final String topic;
    private DefaultMQPushConsumer consumer;

    public OperationErrorEventConsumer(
            ObjectMapper objectMapper,
            OperationErrorLogRepository errorLogRepository,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.operation-error-events}") String topic) {
        this.objectMapper = objectMapper;
        this.errorLogRepository = errorLogRepository;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            consumer = new DefaultMQPushConsumer("operation-error-event-consumer");
            consumer.setNamesrvAddr(nameServer);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.subscribe(topic, "OPERATION_FAILED");
            MessageListenerConcurrently listener = (messages, context) -> {
                try {
                    for (var message : messages) {
                        OperationErrorEvent event = objectMapper.readValue(
                                new String(message.getBody(), StandardCharsets.UTF_8), OperationErrorEvent.class);
                        errorLogRepository.save(
                                event.operation(), event.apiKey(), event.errorMessage(), event.occurredAt());
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception exception) {
                    log.error("Could not persist operation-error event; RocketMQ will retry it", exception);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            };
            consumer.registerMessageListener(listener);
            consumer.start();
            log.info("RocketMQ operation-error consumer subscribed to {}", topic);
        } catch (Exception exception) {
            consumer = null;
            log.warn("RocketMQ operation-error consumer is unavailable; events will not be persisted", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
