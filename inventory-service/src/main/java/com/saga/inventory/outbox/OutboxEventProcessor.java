package com.saga.inventory.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.inventory.entity.OutboxEvent;
import com.saga.inventory.entity.enums.OutboxStatus;
import com.saga.inventory.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventProcessor {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${outbox.poller.send-timeout-ms:5000}")
    private long sendTimeoutMs;

    @Transactional
    public boolean claimAndPublishOne() {
        Optional<OutboxEvent> maybe = outboxEventRepository.claimNext();
        if (maybe.isEmpty()) {
            return false;
        }

        OutboxEvent row = maybe.get();
        try {
            Class<?> eventClass = Class.forName(row.getEventType());
            Object event = objectMapper.readValue(row.getPayload(), eventClass);
            // Send happens before this transaction commits: if the process crashes or the
            // commit fails after the broker acks this send, the row stays PENDING and is
            // resent on the next poll. Broker-level idempotence doesn't cover this (it's a
            // new logical send, not a retry) — duplicates are absorbed by the idempotency-key
            // checks on the consuming side instead.
            kafkaTemplate.send(row.getTopic(), row.getKafkaKey(), event)
                    .get(sendTimeoutMs, TimeUnit.MILLISECONDS);

            row.setStatus(OutboxStatus.PUBLISHED);
            row.setPublishedAt(LocalDateTime.now());
            log.info("Outbox event published: id={}, eventType={}, topic={}", row.getId(), row.getEventType(), row.getTopic());
        } catch (Exception e) {
            row.setRetryCount(row.getRetryCount() + 1);
            row.setLastError(truncate(e.getMessage()));

            if (row.getRetryCount() >= row.getMaxRetries()) {
                row.setStatus(OutboxStatus.FAILED);
                log.error("Outbox event exhausted retries — marking FAILED: id={}, eventType={}, topic={}, retryCount={}",
                        row.getId(), row.getEventType(), row.getTopic(), row.getRetryCount(), e);
            } else {
                long backoffSeconds = Math.min((long) Math.pow(2, row.getRetryCount()), 300);
                row.setNextAttemptAt(LocalDateTime.now().plusSeconds(backoffSeconds));
                log.warn("Outbox publish failed, will retry: id={}, eventType={}, retryCount={}, nextAttemptAt={}",
                        row.getId(), row.getEventType(), row.getRetryCount(), row.getNextAttemptAt(), e);
            }
        }

        outboxEventRepository.save(row);
        return true;
    }

    private String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }
}
