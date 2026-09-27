package com.saga.orchestrator.dto;

import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public record SagaStatusResponse(
        UUID sagaId,
        String orderId,
        String status,
        String currentStep,
        LocalDateTime createdAt,
        LocalDateTime updatedAt,
        List<StepDto> steps
) {
    public record StepDto(
            UUID stepId,
            String stepName,
            String status,
            String errorMessage,
            LocalDateTime createdAt
    ) {
        public static StepDto from(SagaStep step) {
            return new StepDto(
                    step.getId(),
                    step.getStepName().name(),
                    step.getStatus().name(),
                    step.getErrorMessage(),
                    step.getCreatedAt()
            );
        }
    }

    public static SagaStatusResponse from(SagaInstance saga, List<SagaStep> steps) {
        return new SagaStatusResponse(
                saga.getId(),
                saga.getOrderId(),
                saga.getStatus().name(),
                saga.getCurrentStep().name(),
                saga.getCreatedAt(),
                saga.getUpdatedAt(),
                steps.stream().map(StepDto::from).toList()
        );
    }
}
