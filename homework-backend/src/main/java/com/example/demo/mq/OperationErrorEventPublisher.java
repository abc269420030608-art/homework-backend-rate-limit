package com.example.demo.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Publishes failed rule mutations for asynchronous error-log persistence. */
@Component
public class OperationErrorEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OperationErrorEventPublisher.class);

    private final ObjectMapper objectMapper;
    private final String nameServer;
    private final String topic;
    private DefaultMQProducer producer;

    public OperationErrorEventPublisher(
            ObjectMapper objectMapper,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.operation-error-events}") String topic) {
        this.objectMapper = objectMapper;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            producer = new DefaultMQProducer("operation-error-event-producer");
            producer.setNamesrvAddr(nameServer);
            producer.start();
            log.info("RocketMQ operation-error producer connected to {}", nameServer);
        } catch (Exception exception) {
            producer = null;
            log.warn("RocketMQ operation-error producer is unavailable; error events will be skipped", exception);
        }
    }

    public void publish(OperationErrorEvent event) {
        if (producer == null) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(event);
            producer.send(new Message(topic, "OPERATION_FAILED", payload.getBytes(StandardCharsets.UTF_8)));
        } catch (JsonProcessingException exception) {
            log.error("Could not serialize operation-error event", exception);
        } catch (Exception exception) {
            log.warn("Could not publish operation-error event", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
