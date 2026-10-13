package com.example.demo.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Publishes successful rule-operation audits for asynchronous persistence. */
@Component
public class RateLimitOperationEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RateLimitOperationEventPublisher.class);

    private final ObjectMapper objectMapper;
    private final String nameServer;
    private final String topic;
    private DefaultMQProducer producer;

    public RateLimitOperationEventPublisher(
            ObjectMapper objectMapper,
            @Value("${rocketmq.name-server}") String nameServer,
            @Value("${rocketmq.topic.rate-limit-operation-events}") String topic) {
        this.objectMapper = objectMapper;
        this.nameServer = nameServer;
        this.topic = topic;
    }

    @PostConstruct
    void start() {
        try {
            producer = new DefaultMQProducer("rate-limit-operation-event-producer");
            producer.setNamesrvAddr(nameServer);
            producer.setSendMsgTimeout(1_000);
            producer.start();
        } catch (Exception exception) {
            producer = null;
            log.warn("RocketMQ operation-audit producer is unavailable; audit events will be skipped", exception);
        }
    }

    public void publish(RateLimitOperationEvent event) {
        if (producer == null) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(event);
            producer.send(
                    new Message(topic, "RULE_OPERATION", payload.getBytes(StandardCharsets.UTF_8)),
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult result) {
                            // The audit event was accepted by RocketMQ; no request thread was blocked.
                        }

                        @Override
                        public void onException(Throwable exception) {
                            log.warn("Could not publish rate-limit operation event", exception);
                        }
                    });
        } catch (JsonProcessingException exception) {
            log.error("Could not serialize rate-limit operation event", exception);
        } catch (Exception exception) {
            log.warn("Could not publish rate-limit operation event", exception);
        }
    }

    @PreDestroy
    void stop() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
