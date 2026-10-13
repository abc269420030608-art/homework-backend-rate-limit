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

/** Publishes audit events without affecting the result of a rate-limit check. */
@Component
public class RateLimitEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RateLimitEventPublisher.class);

    private final ObjectMapper objectMapper;
    private final String nameServer;
    private final String topic;
    private DefaultMQProducer producer;

    public RateLimitEventPublisher(
            ObjectMapper objectMapper,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.rate-limit-events}") String topic) {
        this.objectMapper = objectMapper;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            producer = new DefaultMQProducer("rate-limit-event-producer");
            producer.setNamesrvAddr(nameServer);
            producer.start();
            log.info("RocketMQ producer connected to {}", nameServer);
        } catch (Exception exception) {
            producer = null;
            log.warn("RocketMQ producer is unavailable; rate-limit events will be skipped", exception);
        }
    }

    public void publish(RateLimitExceededEvent event) {
        if (producer == null) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(event);
            producer.sendOneway(new Message(topic, "RATE_LIMIT_EXCEEDED", payload.getBytes(StandardCharsets.UTF_8)));
        } catch (JsonProcessingException exception) {
            log.error("Could not serialize rate-limit event", exception);
        } catch (Exception exception) {
            log.warn("Could not publish rate-limit event", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
