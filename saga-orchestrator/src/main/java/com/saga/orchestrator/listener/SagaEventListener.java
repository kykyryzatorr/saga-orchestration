package com.saga.orchestrator.listener;

import com.saga.orchestrator.service.SagaOrchestrationService;
import com.saga.shared.event.*;
import com.saga.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaEventListener {

    private final SagaOrchestrationService orchestrationService;

    @KafkaListener(topics = KafkaTopics.ORDER_CREATED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("← order.created: sagaId={}, orderId={}, totalAmount={}",
                event.getSagaId(), event.getOrderId(), event.getTotalAmount());
        orchestrationService.processOrderCreated(event);
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RESERVED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onStockReserved(StockReservedEvent event) {
        log.info("← stock.reserved: sagaId={}, reservationId={}",
                event.getSagaId(), event.getReservationId());
        orchestrationService.processStockReserved(event);
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_PROCESSED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentProcessed(PaymentProcessedEvent event) {
        log.info("← payment.processed: sagaId={}, paymentId={}",
                event.getSagaId(), event.getPaymentId());
        orchestrationService.processPaymentProcessed(event);
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RESERVATION_FAILED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onStockReservationFailed(StockReservationFailedEvent event) {
        log.warn("← stock.reservation.failed: sagaId={}, reason={}",
                event.getSagaId(), event.getReason());
        orchestrationService.processStockReservationFailed(event);
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_FAILED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.warn("← payment.failed: sagaId={}, reason={}",
                event.getSagaId(), event.getReason());
        orchestrationService.processPaymentFailed(event);
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RELEASED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onStockReleased(StockReleasedEvent event) {
        log.info("← stock.released: sagaId={}, reservationId={}",
                event.getSagaId(), event.getReservationId());
        orchestrationService.processStockReleased(event);
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_CANCELLED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCancelled(PaymentCancelledEvent event) {
        log.info("← payment.cancelled: sagaId={}, paymentId={}",
                event.getSagaId(), event.getPaymentId());
        orchestrationService.processPaymentCancelled(event);
    }
}
