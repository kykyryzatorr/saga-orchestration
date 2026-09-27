package com.saga.orchestrator.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventProcessor outboxEventProcessor;

    @Value("${outbox.poller.batch-size:50}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.poller.fixed-delay-ms:2000}")
    public void pollAndPublish() {
        for (int i = 0; i < batchSize; i++) {
            if (!outboxEventProcessor.claimAndPublishOne()) {
                break;
            }
        }
    }
}
