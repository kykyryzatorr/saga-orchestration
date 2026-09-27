package com.saga.orchestrator.grpc;

import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.service.SagaOrchestrationService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class OrchestratorGrpcService extends OrchestratorServiceGrpc.OrchestratorServiceImplBase {

    private final SagaOrchestrationService orchestrationService;

    @Override
    public void startSaga(StartSagaRequest request,
                          StreamObserver<StartSagaResponse> responseObserver) {
        log.info("gRPC startSaga: userId={}, productId={}, quantity={}, unitPrice={} {}",
                request.getUserId(), request.getProductId(),
                request.getQuantity(), request.getUnitPrice(), request.getCurrency());
        try {
            String idempotencyKey = request.getIdempotencyKey().isEmpty()
                    ? null : request.getIdempotencyKey();
            boolean isReplay = orchestrationService.findByIdempotencyKey(idempotencyKey).isPresent();

            SagaInstance saga = orchestrationService.startSaga(
                    request.getUserId(),
                    request.getProductId(),
                    request.getQuantity(),
                    BigDecimal.valueOf(request.getUnitPrice()),
                    request.getCurrency(),
                    idempotencyKey
            );

            StartSagaResponse response = StartSagaResponse.newBuilder()
                    .setSagaId(saga.getId().toString())
                    .setOrderId(saga.getOrderId() != null ? saga.getOrderId() : "")
                    .setStatus(saga.getStatus().name())
                    .setMessage(isReplay
                            ? "Saga already exists for this idempotency key (idempotent replay)"
                            : "Saga started successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("startSaga failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void getSagaStatus(SagaStatusRequest request,
                              StreamObserver<SagaStatusResponse> responseObserver) {
        log.info("gRPC getSagaStatus: sagaId={}", request.getSagaId());
        try {
            SagaInstance saga = orchestrationService.findById(UUID.fromString(request.getSagaId()));

            SagaStatusResponse response = SagaStatusResponse.newBuilder()
                    .setSagaId(saga.getId().toString())
                    .setOrderId(saga.getOrderId() != null ? saga.getOrderId() : "")
                    .setCurrentStep(saga.getCurrentStep().name())
                    .setStatus(saga.getStatus().name())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getSagaStatus failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
