package com.saga.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.inventory.grpc.CheckStockResponse;
import com.saga.inventory.grpc.GetProductResponse;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;
import com.saga.orchestrator.entity.enums.SagaStatus;
import com.saga.orchestrator.entity.enums.SagaStepName;
import com.saga.orchestrator.entity.enums.SagaStepStatus;
import com.saga.orchestrator.entity.OutboxEvent;
import com.saga.orchestrator.entity.enums.OutboxStatus;
import com.saga.orchestrator.grpc.InventoryServiceClient;
import com.saga.orchestrator.model.SagaPayload;
import com.saga.orchestrator.repository.OutboxEventRepository;
import com.saga.orchestrator.repository.SagaInstanceRepository;
import com.saga.orchestrator.repository.SagaStepRepository;
import com.saga.shared.event.*;
import com.saga.shared.exception.IdempotencyConflictException;
import com.saga.shared.kafka.KafkaTopics;
import com.saga.shared.util.RequestHashUtil;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Core saga state machine.
 *
 * Forward flow:
 *   startSaga()
 *     → Kafka → order.create.request
 *     → [SagaEventListener.onOrderCreated()]
 *     → Kafka → stock.reserve.request
 *     → [SagaEventListener.onStockReserved()]
 *     → Kafka → payment.process.request
 *     → [SagaEventListener.onPaymentProcessed()]
 *     → Kafka → order.confirmed
 *     → status = COMPLETED
 *
 * Compensation flow (stock reservation failure):
 *   [SagaEventListener.onStockReservationFailed()]
 *     → Kafka → order.cancelled
 *     → status = FAILED
 *
 * Compensation flow (payment failure):
 *   [SagaEventListener.onPaymentFailed()]
 *     → Kafka → stock.release.request
 *     → [SagaEventListener.onStockReleased()]
 *     → Kafka → order.cancelled
 *     → status = FAILED
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrationService {

    private static final String STEP_STOCK_RESERVE = "STOCK_RESERVE";
    private static final String STEP_PAYMENT_PROCESS = "PAYMENT_PROCESS";
    private static final String STEP_ORDER_CREATE = "ORDER_CREATE";

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaStepRepository sagaStepRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final InventoryServiceClient inventoryServiceClient;

    @Value("${saga.recovery.stuck-threshold-seconds:120}")
    private long stuckThresholdSeconds;

    @Value("${saga.recovery.max-attempts:5}")
    private int maxRecoveryAttempts;

    @Transactional
    public SagaInstance startSaga(String userId, String productId,
                                  Integer quantity, BigDecimal unitPrice, String currency,
                                  String idempotencyKey) {

        String requestHash = RequestHashUtil.sha256(userId, productId, quantity, unitPrice, currency);
        if (idempotencyKey != null) {
            var existing = sagaInstanceRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                if (!requestHash.equals(existing.get().getRequestHash())) {
                    throw new IdempotencyConflictException(
                            "Idempotency key reused with a different request: " + idempotencyKey);
                }
                log.info("Saga start skipped — idempotent replay: idempotencyKey={}, sagaId={}",
                        idempotencyKey, existing.get().getId());
                return existing.get();
            }
        }

        // Advisory stock check via gRPC — fail fast before touching order-service
        CheckStockResponse stockCheck = inventoryServiceClient.checkStock(productId, quantity);
        if (!stockCheck.getAvailable()) {
            log.warn("Saga aborted — insufficient stock: productId={}, {}", productId, stockCheck.getMessage());
            throw new IllegalStateException("Cannot start saga: " + stockCheck.getMessage());
        }

        GetProductResponse productInfo = inventoryServiceClient.getProduct(productId);
        log.info("Product validated via gRPC: name='{}', availableQty={}",
                productInfo.getName(), productInfo.getAvailableQuantity());

        // Pre-generated here (like sagaId) so it can be returned synchronously from
        // POST /api/orders even though order-service's creation reply is now async.
        UUID orderId = UUID.randomUUID();
        // Provisional only — same formula order-service uses, but overwritten with
        // order-service's authoritative value once processOrderCreated() confirms it.
        BigDecimal provisionalTotalAmount = unitPrice.multiply(BigDecimal.valueOf(quantity));

        SagaPayload payload = SagaPayload.builder()
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .unitPrice(unitPrice)
                .totalAmount(provisionalTotalAmount)
                .currency(currency)
                .orderId(orderId)
                .build();

        SagaInstance saga = SagaInstance.builder()
                .orderId(orderId.toString())
                .currentStep(SagaStepName.ORDER_CREATE_REQUESTED)
                .status(SagaStatus.STARTED)
                .payload(toJson(payload))
                .idempotencyKey(idempotencyKey)
                .requestHash(requestHash)
                .build();
        try {
            saga = sagaInstanceRepository.save(saga);
        } catch (DataIntegrityViolationException e) {
            SagaInstance raced = sagaInstanceRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> e);
            if (!requestHash.equals(raced.getRequestHash())) {
                throw new IdempotencyConflictException(
                        "Idempotency key reused with a different request: " + idempotencyKey);
            }
            log.info("Saga start skipped — idempotent replay (race): idempotencyKey={}, sagaId={}",
                    idempotencyKey, raced.getId());
            return raced;
        }

        recordStep(saga, SagaStepName.ORDER_CREATE_REQUESTED, SagaStepStatus.STARTED, null);
        log.info("Saga started: id={}, orderId={} — order creation requested", saga.getId(), orderId);

        publishOrderCreateCommand(saga.getId(), payload);

        return saga;
    }

    @Transactional
    public void processOrderCreated(OrderCreatedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        if (payload.getTotalAmount().compareTo(event.getTotalAmount()) != 0) {
            log.warn("Saga [{}] provisional totalAmount {} != order-service authoritative totalAmount {} "
                            + "— using order-service's value as canonical",
                    saga.getId(), payload.getTotalAmount(), event.getTotalAmount());
        }
        payload.setTotalAmount(event.getTotalAmount());

        recordStep(saga, SagaStepName.ORDER_CREATED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.ORDER_CREATED);
        saga.setPayload(toJson(payload));
        saga.setRecoveryAttempts(0);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] ORDER_CREATED → reserving stock", saga.getId());

        publishStockReserveCommand(saga.getId(), payload);
    }

    @Transactional
    public void processStockReserved(StockReservedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setReservationId(event.getReservationId());

        recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.PAYMENT_PROCESSED);
        saga.setPayload(toJson(payload));
        saga.setRecoveryAttempts(0);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] STOCK_RESERVED → processing payment", saga.getId());

        publishPaymentProcessCommand(saga.getId(), payload);
    }

    @Transactional
    public void processPaymentProcessed(PaymentProcessedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setPaymentId(event.getPaymentId());

        recordStep(saga, SagaStepName.PAYMENT_PROCESSED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.COMPLETED);
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setPayload(toJson(payload));
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] COMPLETED ✓", saga.getId());

        OrderConfirmedEvent confirmed = OrderConfirmedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(payload.getOrderId())
                .paymentId(payload.getPaymentId())
                .reservationId(payload.getReservationId())
                .build();
        confirmed.init();
        outboxEventRepository.save(toOutboxEvent(saga.getId(),
                KafkaTopics.ORDER_CONFIRMED, payload.getOrderId().toString(), confirmed));
    }

    @Transactional
    public void processStockReservationFailed(StockReservationFailedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.FAILED, event.getReason());
        saga.setStatus(SagaStatus.COMPENSATING);
        sagaInstanceRepository.save(saga);
        log.warn("Saga [{}] stock reservation FAILED → cancelling order", saga.getId());

        publishOrderCancelledAndFail(saga, payload.getOrderId(), event.getReason());
    }

    @Transactional
    public void processPaymentFailed(PaymentFailedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setPaymentId(event.getPaymentId());

        recordStep(saga, SagaStepName.PAYMENT_PROCESSED, SagaStepStatus.FAILED, event.getReason());
        saga.setCurrentStep(SagaStepName.STOCK_RELEASED);
        saga.setStatus(SagaStatus.COMPENSATING);
        saga.setPayload(toJson(payload));
        saga.setRecoveryAttempts(0);
        sagaInstanceRepository.save(saga);
        log.warn("Saga [{}] payment FAILED → releasing stock", saga.getId());

        publishStockReleaseCommand(saga, payload, event.getReason());
    }

    @Transactional
    public void processStockReleased(StockReleasedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RELEASED, SagaStepStatus.COMPENSATED, null);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] stock released → cancelling order", saga.getId());

        publishOrderCancelledAndFail(saga, payload.getOrderId(), "Payment failed");
    }

    @Transactional
    public void processPaymentCancelled(PaymentCancelledEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.PAYMENT_CANCELLED, SagaStepStatus.COMPENSATED, null);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] payment cancelled → releasing stock", saga.getId());

        publishStockReleaseCommand(saga, payload, event.getReason());
    }

    // Resends are safe even if the original command was already processed — commands
    // carry sagaId-scoped idempotency keys that downstream services dedupe on.
    @Transactional
    public boolean recoverNextStuckSaga() {
        LocalDateTime staleBefore = LocalDateTime.now().minusSeconds(stuckThresholdSeconds);
        Optional<SagaInstance> maybe = sagaInstanceRepository.claimNextStuck(staleBefore);
        if (maybe.isEmpty()) {
            return false;
        }

        SagaInstance saga = maybe.get();
        SagaPayload payload = fromJson(saga.getPayload());

        if (saga.getStatus() == SagaStatus.STARTED && saga.getCurrentStep() == SagaStepName.ORDER_CREATE_REQUESTED) {
            recoverOrderCreateRequested(saga, payload);
        } else if (saga.getStatus() == SagaStatus.STARTED && saga.getCurrentStep() == SagaStepName.ORDER_CREATED) {
            recoverOrderCreated(saga, payload);
        } else if (saga.getStatus() == SagaStatus.STARTED && saga.getCurrentStep() == SagaStepName.PAYMENT_PROCESSED) {
            recoverPaymentProcessed(saga, payload);
        } else if (saga.getStatus() == SagaStatus.COMPENSATING && saga.getCurrentStep() == SagaStepName.STOCK_RELEASED) {
            recoverStockReleased(saga, payload);
        } else {
            String reason = "Saga recovery: unrecognized stuck state (status=" + saga.getStatus()
                    + ", currentStep=" + saga.getCurrentStep() + ")";
            log.error("Saga [{}] {} — escalating immediately", saga.getId(), reason);
            recordStep(saga, saga.getCurrentStep(), SagaStepStatus.FAILED, reason);
            publishOrderCancelledAndFail(saga, payload.getOrderId(), reason);
        }
        return true;
    }

    private void recoverOrderCreateRequested(SagaInstance saga, SagaPayload payload) {
        saga.setRecoveryAttempts(saga.getRecoveryAttempts() + 1);
        if (saga.getRecoveryAttempts() >= maxRecoveryAttempts) {
            String reason = "Saga recovery: no order-created reply after " + maxRecoveryAttempts + " attempts";
            // Cancel unconditionally: from here we cannot tell "command never delivered" apart
            // from "order was created but the reply was lost" — order.cancelled is a safe,
            // idempotent no-op if order-service never actually created the row.
            log.error("Saga [{}] exhausted recovery attempts waiting on order creation reply — escalating to "
                    + "FAILED and cancelling order defensively", saga.getId());
            recordStep(saga, SagaStepName.ORDER_CREATED, SagaStepStatus.FAILED, reason);
            publishOrderCancelledAndFail(saga, payload.getOrderId(), reason);
        } else {
            sagaInstanceRepository.save(saga);
            log.warn("Saga [{}] stuck waiting on order-created reply — resending OrderCreateCommand (attempt {}/{})",
                    saga.getId(), saga.getRecoveryAttempts(), maxRecoveryAttempts);
            publishOrderCreateCommand(saga.getId(), payload);
        }
    }

    private void recoverOrderCreated(SagaInstance saga, SagaPayload payload) {
        saga.setRecoveryAttempts(saga.getRecoveryAttempts() + 1);
        if (saga.getRecoveryAttempts() >= maxRecoveryAttempts) {
            String reason = "Saga recovery: no stock reservation reply after " + maxRecoveryAttempts + " attempts";
            log.error("Saga [{}] exhausted recovery attempts waiting on stock reservation — escalating to FAILED",
                    saga.getId());
            recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.FAILED, reason);
            publishOrderCancelledAndFail(saga, payload.getOrderId(), reason);
        } else {
            sagaInstanceRepository.save(saga);
            log.warn("Saga [{}] stuck waiting on stock reservation reply — resending StockReserveCommand (attempt {}/{})",
                    saga.getId(), saga.getRecoveryAttempts(), maxRecoveryAttempts);
            publishStockReserveCommand(saga.getId(), payload);
        }
    }

    private void recoverPaymentProcessed(SagaInstance saga, SagaPayload payload) {
        saga.setRecoveryAttempts(saga.getRecoveryAttempts() + 1);
        if (saga.getRecoveryAttempts() >= maxRecoveryAttempts) {
            String reason = "Saga recovery: no payment reply after " + maxRecoveryAttempts + " attempts";
            log.error("Saga [{}] exhausted recovery attempts waiting on payment reply — starting compensation "
                    + "(releasing stock) instead of failing directly, to avoid leaking the reservation", saga.getId());
            recordStep(saga, SagaStepName.PAYMENT_PROCESSED, SagaStepStatus.FAILED, reason);
            saga.setCurrentStep(SagaStepName.STOCK_RELEASED);
            saga.setStatus(SagaStatus.COMPENSATING);
            saga.setRecoveryAttempts(0);
            sagaInstanceRepository.save(saga);
            publishStockReleaseCommand(saga, payload, reason);
        } else {
            sagaInstanceRepository.save(saga);
            log.warn("Saga [{}] stuck waiting on payment reply — resending PaymentProcessCommand (attempt {}/{})",
                    saga.getId(), saga.getRecoveryAttempts(), maxRecoveryAttempts);
            publishPaymentProcessCommand(saga.getId(), payload);
        }
    }

    private void recoverStockReleased(SagaInstance saga, SagaPayload payload) {
        saga.setRecoveryAttempts(saga.getRecoveryAttempts() + 1);
        if (saga.getRecoveryAttempts() >= maxRecoveryAttempts) {
            String reason = "Saga recovery: no stock release confirmation after " + maxRecoveryAttempts + " attempts";
            log.error("Saga [{}] exhausted recovery attempts waiting on stock release confirmation — escalating to "
                    + "FAILED (reservation may remain held; needs manual reconciliation)", saga.getId());
            recordStep(saga, SagaStepName.STOCK_RELEASED, SagaStepStatus.FAILED, reason);
            publishOrderCancelledAndFail(saga, payload.getOrderId(), reason);
        } else {
            sagaInstanceRepository.save(saga);
            log.warn("Saga [{}] stuck waiting on stock release confirmation — resending StockReleaseCommand (attempt {}/{})",
                    saga.getId(), saga.getRecoveryAttempts(), maxRecoveryAttempts);
            publishStockReleaseCommand(saga, payload, "Saga recovery retry");
        }
    }

    @Transactional(readOnly = true)
    public Optional<SagaInstance> findByIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null) {
            return Optional.empty();
        }
        return sagaInstanceRepository.findByIdempotencyKey(idempotencyKey);
    }

    @Transactional(readOnly = true)
    public SagaInstance findById(UUID sagaId) {
        return sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new EntityNotFoundException("Saga not found: " + sagaId));
    }

    @Transactional(readOnly = true)
    public List<SagaStep> findStepsBySagaId(UUID sagaId) {
        return sagaStepRepository.findBySagaInstanceIdOrderByCreatedAtAsc(sagaId);
    }

    private void publishOrderCreateCommand(UUID sagaId, SagaPayload payload) {
        OrderCreateCommand cmd = OrderCreateCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(payload.getOrderId())
                .userId(payload.getUserId())
                .productId(payload.getProductId())
                .quantity(payload.getQuantity())
                .unitPrice(payload.getUnitPrice())
                .idempotencyKey(sagaId + ":" + STEP_ORDER_CREATE)
                .build();
        cmd.init();
        outboxEventRepository.save(toOutboxEvent(sagaId,
                KafkaTopics.ORDER_CREATE_REQUEST, payload.getOrderId().toString(), cmd));
        log.info("Saga [{}] → enqueued OrderCreateCommand to outbox", sagaId);
    }

    private void publishStockReserveCommand(UUID sagaId, SagaPayload payload) {
        StockReserveCommand cmd = StockReserveCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(payload.getOrderId())
                .productId(UUID.fromString(payload.getProductId()))
                .quantity(payload.getQuantity())
                .idempotencyKey(sagaId + ":" + STEP_STOCK_RESERVE)
                .build();
        cmd.init();
        outboxEventRepository.save(toOutboxEvent(sagaId,
                KafkaTopics.STOCK_RESERVE_REQUEST, payload.getOrderId().toString(), cmd));
        log.info("Saga [{}] → enqueued StockReserveCommand to outbox", sagaId);
    }

    private void publishPaymentProcessCommand(UUID sagaId, SagaPayload payload) {
        PaymentProcessCommand cmd = PaymentProcessCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(payload.getOrderId())
                .userId(payload.getUserId())
                .totalAmount(payload.getTotalAmount())
                .currency(payload.getCurrency())
                .idempotencyKey(sagaId + ":" + STEP_PAYMENT_PROCESS)
                .build();
        cmd.init();
        outboxEventRepository.save(toOutboxEvent(sagaId,
                KafkaTopics.PAYMENT_PROCESS_REQUEST, payload.getOrderId().toString(), cmd));
        log.info("Saga [{}] → enqueued PaymentProcessCommand to outbox", sagaId);
    }

    private void publishStockReleaseCommand(SagaInstance saga, SagaPayload payload, String reason) {
        StockReleaseCommand cmd = StockReleaseCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(payload.getOrderId())
                .reservationId(payload.getReservationId())
                .reason(reason)
                .build();
        cmd.init();
        outboxEventRepository.save(toOutboxEvent(saga.getId(),
                KafkaTopics.STOCK_RELEASE_REQUEST, payload.getOrderId().toString(), cmd));
        log.info("Saga [{}] → enqueued StockReleaseCommand to outbox (reason={})", saga.getId(), reason);
    }

    private void publishOrderCancelledAndFail(SagaInstance saga, UUID orderId, String reason) {
        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(orderId)
                .reason(reason)
                .build();
        event.init();
        outboxEventRepository.save(toOutboxEvent(saga.getId(),
                KafkaTopics.ORDER_CANCELLED, orderId.toString(), event));

        recordStep(saga, SagaStepName.ORDER_CANCELLED, SagaStepStatus.COMPENSATED, reason);
        saga.setCurrentStep(SagaStepName.ORDER_CANCELLED);
        saga.setStatus(SagaStatus.FAILED);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] FAILED — order cancelled, reason={}", saga.getId(), reason);
    }

    private SagaInstance findBySagaId(UUID sagaId) {
        return sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new EntityNotFoundException("Saga not found: " + sagaId));
    }

    private void recordStep(SagaInstance saga, SagaStepName stepName,
                             SagaStepStatus status, String errorMessage) {
        sagaStepRepository.save(SagaStep.builder()
                .sagaInstance(saga)
                .stepName(stepName)
                .status(status)
                .errorMessage(errorMessage)
                .build());
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga payload", e);
        }
    }

    private SagaPayload fromJson(String json) {
        try {
            return objectMapper.readValue(json, SagaPayload.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize saga payload", e);
        }
    }

    private OutboxEvent toOutboxEvent(UUID sagaId, String topic, String kafkaKey, BaseEvent event) {
        try {
            return OutboxEvent.builder()
                    .aggregateType("SagaInstance")
                    .aggregateId(sagaId.toString())
                    .eventType(event.getClass().getName())
                    .topic(topic)
                    .kafkaKey(kafkaKey)
                    .payload(objectMapper.writeValueAsString(event))
                    .status(OutboxStatus.PENDING)
                    .build();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event for outbox: " + event.getClass(), e);
        }
    }
}
