package com.saga.orchestrator.controller;

import com.saga.orchestrator.dto.SagaStatusResponse;
import com.saga.orchestrator.dto.StartOrderRequest;
import com.saga.orchestrator.dto.StartOrderResponse;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;
import com.saga.orchestrator.service.SagaOrchestrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SagaController {

    private final SagaOrchestrationService orchestrationService;

    @PostMapping("/api/orders")
    public ResponseEntity<StartOrderResponse> startOrder(
            @Valid @RequestBody StartOrderRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        log.info("POST /api/orders: userId={}, productId={}, qty={}, unitPrice={} {}",
                request.userId(), request.productId(), request.quantity(),
                request.unitPrice(), request.currency());

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            log.warn("Saga start rejected: missing Idempotency-Key header");
            return ResponseEntity.badRequest()
                    .body(new StartOrderResponse(null, null, null, "Idempotency-Key header is required."));
        }

        boolean isReplay = orchestrationService.findByIdempotencyKey(idempotencyKey).isPresent();

        SagaInstance saga = orchestrationService.startSaga(
                request.userId(),
                request.productId(),
                request.quantity(),
                request.unitPrice(),
                request.currency(),
                idempotencyKey
        );

        return ResponseEntity
                .status(isReplay ? HttpStatus.OK : HttpStatus.CREATED)
                .body(new StartOrderResponse(
                        saga.getId(),
                        saga.getOrderId(),
                        saga.getStatus().name(),
                        isReplay
                                ? "Saga already exists for this idempotency key (idempotent replay)."
                                : "Saga started. Poll GET /api/sagas/" + saga.getId() + " for status."
                ));
    }

    // Maps business rule violations (e.g. insufficient stock) to 400 instead of 500
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handleIllegalState(IllegalStateException ex) {
        log.warn("Saga start rejected: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    @GetMapping("/api/sagas/{sagaId}")
    public ResponseEntity<SagaStatusResponse> getSagaStatus(@PathVariable UUID sagaId) {
        log.info("GET /api/sagas/{}", sagaId);

        SagaInstance saga = orchestrationService.findById(sagaId);
        List<SagaStep> steps = orchestrationService.findStepsBySagaId(sagaId);

        return ResponseEntity.ok(SagaStatusResponse.from(saga, steps));
    }
}
