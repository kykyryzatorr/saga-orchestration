package com.saga.inventory.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.inventory.entity.OutboxEvent;
import com.saga.inventory.entity.Product;
import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.entity.enums.OutboxStatus;
import com.saga.inventory.entity.enums.ReservationStatus;
import com.saga.inventory.outbox.OutboxEventRecorder;
import com.saga.inventory.repository.OutboxEventRepository;
import com.saga.inventory.repository.ProductRepository;
import com.saga.inventory.repository.StockReservationRepository;
import com.saga.shared.event.BaseEvent;
import com.saga.shared.event.StockReleasedEvent;
import com.saga.shared.event.StockReservationFailedEvent;
import com.saga.shared.event.StockReservedEvent;
import com.saga.shared.exception.IdempotencyConflictException;
import com.saga.shared.kafka.KafkaTopics;
import com.saga.shared.util.RequestHashUtil;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final ProductRepository productRepository;
    private final StockReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OutboxEventRecorder outboxEventRecorder;
    private final ObjectMapper objectMapper;

    @Transactional
    public StockReservation reserveStock(UUID sagaId, String orderId, UUID productId, Integer quantity,
                                         String idempotencyKey) {
        String requestHash = RequestHashUtil.sha256(orderId, productId, quantity);
        if (idempotencyKey != null) {
            var existing = reservationRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                if (!requestHash.equals(existing.get().getRequestHash())) {
                    throw new IdempotencyConflictException(
                            "Idempotency key reused with a different request: " + idempotencyKey);
                }
                log.info("Stock reservation skipped — idempotent replay: idempotencyKey={}, reservationId={}",
                        idempotencyKey, existing.get().getId());
                return existing.get();
            }
        }

        // Pessimistic lock to prevent concurrent over-reservation
        Product product = productRepository.findByIdWithLock(productId)
                .orElseThrow(() -> new EntityNotFoundException("Product not found: " + productId));

        if (product.getAvailableQuantity() < quantity) {
            log.warn("Insufficient stock: productId={}, available={}, requested={}",
                    productId, product.getAvailableQuantity(), quantity);

            StockReservationFailedEvent event = StockReservationFailedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(sagaId)
                    .orderId(UUID.fromString(orderId))
                    .productId(productId)
                    .requestedQuantity(quantity)
                    .availableQuantity(product.getAvailableQuantity())
                    .reason("Insufficient stock")
                    .build();
            event.init();
            outboxEventRecorder.recordInNewTransaction(toOutboxEvent("StockReservation", null,
                    KafkaTopics.STOCK_RESERVATION_FAILED, orderId, event));
            throw new IllegalStateException("Insufficient stock for product: " + productId);
        }

        product.setReservedQuantity(product.getReservedQuantity() + quantity);
        productRepository.save(product);

        StockReservation reservation = StockReservation.builder()
                .orderId(orderId)
                .product(product)
                .quantity(quantity)
                .status(ReservationStatus.RESERVED)
                .idempotencyKey(idempotencyKey)
                .requestHash(requestHash)
                .build();
        try {
            reservation = reservationRepository.save(reservation);
        } catch (DataIntegrityViolationException e) {
            StockReservation raced = reservationRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> e);
            if (!requestHash.equals(raced.getRequestHash())) {
                throw new IdempotencyConflictException(
                        "Idempotency key reused with a different request: " + idempotencyKey);
            }
            log.info("Stock reservation skipped — idempotent replay (race): idempotencyKey={}, reservationId={}",
                    idempotencyKey, raced.getId());
            return raced;
        }
        log.info("Stock reserved: reservationId={}, productId={}, quantity={}", reservation.getId(), productId, quantity);

        StockReservedEvent event = StockReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(UUID.fromString(orderId))
                .reservationId(reservation.getId())
                .productId(productId)
                .quantity(quantity)
                .build();
        event.init();
        outboxEventRepository.save(toOutboxEvent("StockReservation", reservation.getId().toString(),
                KafkaTopics.STOCK_RESERVED, orderId, event));
        return reservation;
    }

    @Transactional
    public StockReservation releaseStock(UUID sagaId, UUID reservationId, String reason) {
        StockReservation reservation = reservationRepository.findByIdWithLock(reservationId)
                .orElseThrow(() -> new EntityNotFoundException("Reservation not found: " + reservationId));

        if (reservation.getStatus() == ReservationStatus.RELEASED) {
            log.info("Stock release skipped — idempotent replay: reservationId={}", reservationId);
            return reservation;
        }

        Product product = productRepository.findByIdWithLock(reservation.getProduct().getId())
                .orElseThrow(() -> new EntityNotFoundException("Product not found"));

        product.setReservedQuantity(product.getReservedQuantity() - reservation.getQuantity());
        productRepository.save(product);

        reservation.setStatus(ReservationStatus.RELEASED);
        reservation = reservationRepository.save(reservation);
        log.info("Stock released: reservationId={}, reason={}", reservationId, reason);

        StockReleasedEvent event = StockReleasedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(UUID.fromString(reservation.getOrderId()))
                .reservationId(reservation.getId())
                .build();
        event.init();
        outboxEventRepository.save(toOutboxEvent("StockReservation", reservation.getId().toString(),
                KafkaTopics.STOCK_RELEASED, reservation.getOrderId(), event));
        return reservation;
    }

    @Transactional
    public Product addProduct(String name, Integer quantity) {
        Product product = Product.builder()
                .name(name)
                .quantity(quantity)
                .reservedQuantity(0)
                .build();
        product = productRepository.save(product);
        log.info("Product added: id={}, name={}, quantity={}", product.getId(), name, quantity);
        return product;
    }

    @Transactional(readOnly = true)
    public Product findProductById(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new EntityNotFoundException("Product not found: " + productId));
    }

    @Transactional(readOnly = true)
    public List<Product> findAllProducts() {
        return productRepository.findAll();
    }

    @Transactional(readOnly = true)
    public StockReservation findReservationByOrderId(String orderId) {
        return reservationRepository.findByOrderId(orderId)
                .orElseThrow(() -> new EntityNotFoundException("Reservation not found for order: " + orderId));
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
