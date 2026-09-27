package com.saga.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.saga.order.grpc.GetOrderResponse;
import com.saga.payment.entity.OutboxEvent;
import com.saga.payment.entity.Payment;
import com.saga.payment.entity.enums.PaymentStatus;
import com.saga.payment.grpc.OrderServiceClient;
import com.saga.payment.repository.OutboxEventRepository;
import com.saga.payment.repository.PaymentRepository;
import com.saga.shared.event.PaymentFailedEvent;
import com.saga.shared.event.PaymentProcessedEvent;
import com.saga.shared.kafka.KafkaTopics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;
    @Mock
    private OutboxEventRepository outboxEventRepository;
    @Mock
    private OrderServiceClient orderServiceClient;

    private ObjectMapper objectMapper;
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        paymentService = new PaymentService(
                paymentRepository, outboxEventRepository, objectMapper, orderServiceClient);
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> {
            Payment payment = invocation.getArgument(0);
            if (payment.getId() == null) {
                payment.setId(UUID.randomUUID());
            }
            return payment;
        });
    }

    @Test
    void processPayment_whenTotalMatchesOrder_processesCanonicalTotal() throws Exception {
        UUID sagaId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        when(paymentRepository.findByIdempotencyKey("payment-key")).thenReturn(Optional.empty());
        when(orderServiceClient.getOrder(orderId.toString())).thenReturn(order(orderId, 20.00));

        Payment result = paymentService.processPayment(
                sagaId, orderId.toString(), "user-1", new BigDecimal("20.00"), "USD", "payment-key");

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
        assertThat(result.getTotalAmount()).isEqualByComparingTo("20.00");
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        org.mockito.Mockito.verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.PAYMENT_PROCESSED);
        PaymentProcessedEvent event = objectMapper.readValue(
                captor.getValue().getPayload(), PaymentProcessedEvent.class);
        assertThat(event.getTotalAmount()).isEqualByComparingTo("20.00");
    }

    @Test
    void processPayment_whenTotalDiffersFromOrder_failsAndPublishesFailure() throws Exception {
        UUID sagaId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        when(paymentRepository.findByIdempotencyKey("payment-key")).thenReturn(Optional.empty());
        when(orderServiceClient.getOrder(orderId.toString())).thenReturn(order(orderId, 20.00));

        Payment result = paymentService.processPayment(
                sagaId, orderId.toString(), "user-1", new BigDecimal("10.00"), "USD", "payment-key");

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(result.getFailureReason()).contains("requested=10.00", "orderTotal=20.0");
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        org.mockito.Mockito.verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getTopic()).isEqualTo(KafkaTopics.PAYMENT_FAILED);
        PaymentFailedEvent event = objectMapper.readValue(
                captor.getValue().getPayload(), PaymentFailedEvent.class);
        assertThat(event.getTotalAmount()).isEqualByComparingTo("10.00");
        assertThat(event.getReason()).contains("does not match order total");
    }

    @Test
    void processPayment_atGatewayThreshold_failsUsingTotalAmount() {
        UUID orderId = UUID.randomUUID();
        when(orderServiceClient.getOrder(orderId.toString())).thenReturn(order(orderId, 10_000.00));

        Payment result = paymentService.processPayment(
                UUID.randomUUID(), orderId.toString(), "user-1",
                new BigDecimal("10000.00"), "USD", null);

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(result.getFailureReason()).isEqualTo("Payment declined by gateway");
    }

    @Test
    void processPayment_belowGatewayThreshold_succeedsUsingTotalAmount() {
        UUID orderId = UUID.randomUUID();
        when(orderServiceClient.getOrder(orderId.toString())).thenReturn(order(orderId, 9_999.99));

        Payment result = paymentService.processPayment(
                UUID.randomUUID(), orderId.toString(), "user-1",
                new BigDecimal("9999.99"), "USD", null);

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
    }

    private GetOrderResponse order(UUID orderId, double totalAmount) {
        return GetOrderResponse.newBuilder()
                .setOrderId(orderId.toString())
                .setUserId("user-1")
                .setStatus("PENDING")
                .setTotalAmount(totalAmount)
                .build();
    }
}
