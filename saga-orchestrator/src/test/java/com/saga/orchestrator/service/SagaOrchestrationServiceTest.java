package com.saga.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.saga.orchestrator.entity.OutboxEvent;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.enums.SagaStatus;
import com.saga.orchestrator.entity.enums.SagaStepName;
import com.saga.orchestrator.grpc.InventoryServiceClient;
import com.saga.orchestrator.model.SagaPayload;
import com.saga.orchestrator.repository.OutboxEventRepository;
import com.saga.orchestrator.repository.SagaInstanceRepository;
import com.saga.orchestrator.repository.SagaStepRepository;
import com.saga.inventory.grpc.CheckStockResponse;
import com.saga.inventory.grpc.GetProductResponse;
import com.saga.shared.event.OrderCreateCommand;
import com.saga.shared.event.OrderCreatedEvent;
import com.saga.shared.event.PaymentFailedEvent;
import com.saga.shared.event.PaymentProcessCommand;
import com.saga.shared.event.StockReservedEvent;
import com.saga.shared.kafka.KafkaTopics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SagaOrchestrationServiceTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;
    @Mock
    private SagaStepRepository sagaStepRepository;
    @Mock
    private OutboxEventRepository outboxEventRepository;
    @Mock
    private InventoryServiceClient inventoryServiceClient;

    private ObjectMapper objectMapper;
    private SagaOrchestrationService sagaOrchestrationService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        sagaOrchestrationService = new SagaOrchestrationService(sagaInstanceRepository, sagaStepRepository,
                outboxEventRepository, objectMapper, inventoryServiceClient);
        ReflectionTestUtils.setField(sagaOrchestrationService, "stuckThresholdSeconds", 120L);
        ReflectionTestUtils.setField(sagaOrchestrationService, "maxRecoveryAttempts", 5);
    }

    private SagaInstance stuckSaga(SagaStatus status, SagaStepName currentStep, int recoveryAttempts) {
        SagaPayload payload = SagaPayload.builder()
                .userId("user-1")
                .productId(UUID.randomUUID().toString())
                .quantity(2)
                .unitPrice(BigDecimal.TEN)
                .totalAmount(BigDecimal.valueOf(20))
                .currency("USD")
                .orderId(UUID.randomUUID())
                .reservationId(UUID.randomUUID())
                .build();
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return SagaInstance.builder()
                .id(UUID.randomUUID())
                .orderId(payload.getOrderId().toString())
                .status(status)
                .currentStep(currentStep)
                .payload(json)
                .recoveryAttempts(recoveryAttempts)
                .build();
    }

    @Test
    void startSaga_persistsSagaAndDispatchesOrderCreateCommandWithProvisionalTotal() throws Exception {
        String productId = UUID.randomUUID().toString();
        when(inventoryServiceClient.checkStock(productId, 2)).thenReturn(
                CheckStockResponse.newBuilder().setAvailable(true).build());
        when(inventoryServiceClient.getProduct(productId)).thenReturn(
                GetProductResponse.newBuilder().setName("Product").build());
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(invocation -> {
            SagaInstance saga = invocation.getArgument(0);
            saga.setId(UUID.randomUUID());
            return saga;
        });

        SagaInstance saga = sagaOrchestrationService.startSaga(
                "user-1", productId, 2, new BigDecimal("10.00"), "USD", null);

        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CREATE_REQUESTED);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        SagaPayload payload = objectMapper.readValue(saga.getPayload(), SagaPayload.class);
        assertThat(payload.getOrderId()).isNotNull();
        assertThat(payload.getUnitPrice()).isEqualByComparingTo("10.00");
        assertThat(payload.getTotalAmount()).isEqualByComparingTo("20.00");

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.ORDER_CREATE_REQUEST);
        OrderCreateCommand command = objectMapper.readValue(captor.getValue().getPayload(), OrderCreateCommand.class);
        assertThat(command.getOrderId()).isEqualTo(payload.getOrderId());
        assertThat(command.getUnitPrice()).isEqualByComparingTo("10.00");
    }

    @Test
    void processOrderCreated_advancesToOrderCreatedAndDispatchesStockReserveCommandWithAuthoritativeTotal() throws Exception {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATE_REQUESTED, 3);
        UUID sagaId = saga.getId();
        when(sagaInstanceRepository.findById(sagaId)).thenReturn(Optional.of(saga));

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(UUID.fromString(saga.getOrderId()))
                .totalAmount(new BigDecimal("999.99"))
                .build();
        event.init();

        sagaOrchestrationService.processOrderCreated(event);

        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CREATED);
        assertThat(saga.getRecoveryAttempts()).isEqualTo(0);
        SagaPayload payload = objectMapper.readValue(saga.getPayload(), SagaPayload.class);
        assertThat(payload.getTotalAmount()).isEqualByComparingTo("999.99");

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.STOCK_RESERVE_REQUEST);
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtOrderCreateRequestedUnderMaxAttempts_resendsOrderCreateCommandAndIncrementsAttempts() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATE_REQUESTED, 1);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getRecoveryAttempts()).isEqualTo(2);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.ORDER_CREATE_REQUEST);
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtOrderCreateRequestedAtMaxAttempts_escalatesViaOrderCancelledAndFail() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATE_REQUESTED, 4);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CANCELLED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.ORDER_CANCELLED);
    }

    @Test
    void recoverNextStuckSaga_whenNoStuckSagaFound_returnsFalseWithNoSideEffects() {
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.empty());

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isFalse();
        verify(sagaInstanceRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtOrderCreatedUnderMaxAttempts_resendsStockReserveCommandAndIncrementsAttempts() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATED, 1);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getRecoveryAttempts()).isEqualTo(2);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        verify(sagaInstanceRepository).save(saga);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.STOCK_RESERVE_REQUEST);
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtOrderCreatedAtMaxAttempts_escalatesViaOrderCancelledAndFail() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATED, 4);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CANCELLED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.ORDER_CANCELLED);
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtPaymentProcessedUnderMaxAttempts_resendsPaymentProcessCommandAndIncrementsAttempts() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.PAYMENT_PROCESSED, 0);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getRecoveryAttempts()).isEqualTo(1);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.PAYMENT_PROCESS_REQUEST);
        try {
            PaymentProcessCommand command = objectMapper.readValue(
                    captor.getValue().getPayload(), PaymentProcessCommand.class);
            assertThat(command.getTotalAmount()).isEqualByComparingTo("20.00");
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtPaymentProcessedAtMaxAttempts_startsCompensationByReleasingStockInsteadOfFailingDirectly() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.PAYMENT_PROCESSED, 4);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.STOCK_RELEASED);
        assertThat(saga.getRecoveryAttempts()).isEqualTo(0);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.STOCK_RELEASE_REQUEST);
        assertThat(captor.getAllValues())
                .noneMatch(e -> e.getTopic().equals(KafkaTopics.ORDER_CANCELLED));
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtStockReleasedUnderMaxAttempts_resendsStockReleaseCommand() {
        SagaInstance saga = stuckSaga(SagaStatus.COMPENSATING, SagaStepName.STOCK_RELEASED, 2);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getRecoveryAttempts()).isEqualTo(3);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.STOCK_RELEASE_REQUEST);
    }

    @Test
    void recoverNextStuckSaga_whenStuckAtStockReleasedAtMaxAttempts_escalatesViaOrderCancelledAndFail() {
        SagaInstance saga = stuckSaga(SagaStatus.COMPENSATING, SagaStepName.STOCK_RELEASED, 4);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CANCELLED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.ORDER_CANCELLED);
    }

    @Test
    void recoverNextStuckSaga_whenUnrecognizedStateCombo_escalatesImmediatelyRegardlessOfAttempts() {
        SagaInstance saga = stuckSaga(SagaStatus.COMPENSATING, SagaStepName.PAYMENT_CANCELLED, 0);
        when(sagaInstanceRepository.claimNextStuck(any())).thenReturn(Optional.of(saga));

        boolean result = sagaOrchestrationService.recoverNextStuckSaga();

        assertThat(result).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.ORDER_CANCELLED);
    }

    @Test
    void processStockReserved_resetsRecoveryAttemptsToZero() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.ORDER_CREATED, 3);
        UUID sagaId = saga.getId();
        when(sagaInstanceRepository.findById(sagaId)).thenReturn(Optional.of(saga));

        StockReservedEvent event = StockReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .reservationId(UUID.randomUUID())
                .build();
        event.init();

        sagaOrchestrationService.processStockReserved(event);

        assertThat(saga.getRecoveryAttempts()).isEqualTo(0);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.PAYMENT_PROCESSED);
    }

    @Test
    void processPaymentFailed_resetsRecoveryAttemptsToZeroAndPublishesStockReleaseCommand() {
        SagaInstance saga = stuckSaga(SagaStatus.STARTED, SagaStepName.PAYMENT_PROCESSED, 3);
        UUID sagaId = saga.getId();
        when(sagaInstanceRepository.findById(sagaId)).thenReturn(Optional.of(saga));

        PaymentFailedEvent event = PaymentFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .paymentId(UUID.randomUUID())
                .reason("card declined")
                .build();
        event.init();

        sagaOrchestrationService.processPaymentFailed(event);

        assertThat(saga.getRecoveryAttempts()).isEqualTo(0);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStepName.STOCK_RELEASED);
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.STOCK_RELEASE_REQUEST);
    }
}
