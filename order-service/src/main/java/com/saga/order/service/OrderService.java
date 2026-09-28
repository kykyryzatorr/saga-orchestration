package com.saga.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.order.entity.Order;
import com.saga.order.entity.OutboxEvent;
import com.saga.order.entity.enums.OrderStatus;
import com.saga.order.entity.enums.OutboxStatus;
import com.saga.order.repository.OrderRepository;
import com.saga.order.repository.OutboxEventRepository;
import com.saga.shared.event.BaseEvent;
import com.saga.shared.event.OrderCreatedEvent;
import com.saga.shared.exception.IdempotencyConflictException;
import com.saga.shared.kafka.KafkaTopics;
import com.saga.shared.util.RequestHashUtil;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Order createOrder(UUID orderId, UUID sagaId, String userId, String productId,
                             Integer quantity, BigDecimal unitPrice, String idempotencyKey) {
        String requestHash = RequestHashUtil.sha256(userId, productId, quantity, unitPrice);
        if (idempotencyKey != null) {
            var existing = orderRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                if (!requestHash.equals(existing.get().getRequestHash())) {
                    throw new IdempotencyConflictException(
                            "Idempotency key reused with a different request: " + idempotencyKey);
                }
                log.info("Order creation skipped — idempotent replay: idempotencyKey={}, orderId={}",
                        idempotencyKey, existing.get().getId());
                return existing.get();
            }
        }

        BigDecimal totalAmount = unitPrice.multiply(BigDecimal.valueOf(quantity));

        Order order = Order.builder()
                .id(orderId)
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .unitPrice(unitPrice)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .requestHash(requestHash)
                .build();

        try {
            order = orderRepository.save(order);
        } catch (DataIntegrityViolationException e) {
            Order raced = orderRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> e);
            if (!requestHash.equals(raced.getRequestHash())) {
                throw new IdempotencyConflictException(
                        "Idempotency key reused with a different request: " + idempotencyKey);
            }
            log.info("Order creation skipped — idempotent replay (race): idempotencyKey={}, orderId={}",
                    idempotencyKey, raced.getId());
            return raced;
        }
        log.info("Order created: id={}, userId={}, productId={}", order.getId(), userId, productId);

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(order.getId())
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .unitPrice(unitPrice)
                .totalAmount(totalAmount)
                .build();
        event.init();

        outboxEventRepository.save(toOutboxEvent("Order", order.getId().toString(),
                KafkaTopics.ORDER_CREATED, order.getId().toString(), event));
        log.info("OrderCreatedEvent enqueued to outbox: orderId={}", order.getId());

        return order;
    }

    @Transactional
    public Order confirmOrder(UUID orderId) {
        Order order = findById(orderId);
        order.setStatus(OrderStatus.CONFIRMED);
        order = orderRepository.save(order);
        log.info("Order confirmed: id={}", orderId);
        return order;
    }

    @Transactional
    public Order cancelOrder(UUID orderId, String reason) {
        Order order = findById(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order = orderRepository.save(order);
        log.info("Order cancelled: id={}, reason={}", orderId, reason);
        return order;
    }

    @Transactional(readOnly = true)
    public Order findById(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new EntityNotFoundException("Order not found: " + orderId));
    }

    @Transactional(readOnly = true)
    public List<Order> findByUserId(String userId) {
        return orderRepository.findByUserId(userId);
    }

    @Transactional(readOnly = true)
    public List<Order> findAll() {
        return orderRepository.findAll();
    }

    private OutboxEvent toOutboxEvent(String aggregateType, String aggregateId,
                                      String topic, String kafkaKey, BaseEvent event) {
        try {
            return OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
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
