package com.saga.orchestrator.recovery;

import com.saga.orchestrator.service.SagaOrchestrationService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SagaRecoveryScheduler {

    private final SagaOrchestrationService sagaOrchestrationService;

    @Value("${saga.recovery.batch-size:20}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${saga.recovery.fixed-delay-ms:30000}")
    public void recoverStuckSagas() {
        for (int i = 0; i < batchSize; i++) {
            if (!sagaOrchestrationService.recoverNextStuckSaga()) {
                break;
            }
        }
    }
}
