package com.saga.order.listener;

import com.saga.order.service.OrderService;
import com.saga.shared.event.OrderCancelledEvent;
import com.saga.shared.event.OrderConfirmedEvent;
import com.saga.shared.event.OrderCreateCommand;
import com.saga.shared.exception.IdempotencyConflictException;
import com.saga.shared.kafka.KafkaTopics;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER_CREATE_REQUEST,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreateRequest(OrderCreateCommand cmd) {
        log.info("← order.create.request: sagaId={}, orderId={}, userId={}, productId={}",
                cmd.getSagaId(), cmd.getOrderId(), cmd.getUserId(), cmd.getProductId());
        try {
            orderService.createOrder(cmd.getOrderId(), cmd.getSagaId(), cmd.getUserId(),
                    cmd.getProductId(), cmd.getQuantity(), cmd.getUnitPrice(), cmd.getIdempotencyKey());
        } catch (IdempotencyConflictException e) {
            // Should be unreachable: the orchestrator always resends this command with identical
            // business fields (derived from SagaPayload, never mutated) under the same deterministic
            // sagaId:ORDER_CREATE key. A real conflict here would indicate an orchestrator bug, not
            // a legitimate business rejection — log loudly instead of retrying (a hash mismatch
            // won't resolve itself) or crashing the consumer.
            log.error("Order creation idempotency conflict — should be unreachable: sagaId={}, {}",
                    cmd.getSagaId(), e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CONFIRMED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        log.info("← order.confirmed: sagaId={}, orderId={}",
                event.getSagaId(), event.getOrderId());
        orderService.confirmOrder(event.getOrderId());
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CANCELLED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(OrderCancelledEvent event) {
        log.warn("← order.cancelled: sagaId={}, orderId={}, reason={}",
                event.getSagaId(), event.getOrderId(), event.getReason());
        try {
            orderService.cancelOrder(event.getOrderId(), event.getReason());
        } catch (EntityNotFoundException e) {
            // Possible under async order creation: OrderCreateCommand may never have reached
            // order-service (lost message, or saga recovery escalated straight to cancellation
            // after exhausting retries) before the saga gave up. Nothing to cancel — this is a
            // valid terminal state, not an error.
            log.info("Order cancellation skipped — order was never created: orderId={}", event.getOrderId());
        }
    }
}
