package com.saga.inventory.listener;

import com.saga.inventory.service.InventoryService;
import com.saga.shared.event.StockReleaseCommand;
import com.saga.shared.event.StockReserveCommand;
import com.saga.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventListener {

    private final InventoryService inventoryService;

    @KafkaListener(topics = KafkaTopics.STOCK_RESERVE_REQUEST,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onStockReserveRequest(StockReserveCommand cmd) {
        log.info("← stock.reserve.request: sagaId={}, orderId={}, productId={}, qty={}",
                cmd.getSagaId(), cmd.getOrderId(), cmd.getProductId(), cmd.getQuantity());
        try {
            inventoryService.reserveStock(
                    cmd.getSagaId(),
                    cmd.getOrderId().toString(),
                    cmd.getProductId(),
                    cmd.getQuantity(),
                    cmd.getIdempotencyKey()
            );
        } catch (IllegalStateException e) {
            // Business failure (insufficient stock) — StockReservationFailedEvent already
            // published inside reserveStock(). Infra exceptions propagate to the Kafka
            // error handler for retry/DLQ instead of being swallowed here.
            log.warn("Stock reservation failed for sagaId={}: {}", cmd.getSagaId(), e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RELEASE_REQUEST,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onStockReleaseRequest(StockReleaseCommand cmd) {
        log.info("← stock.release.request: sagaId={}, reservationId={}, reason={}",
                cmd.getSagaId(), cmd.getReservationId(), cmd.getReason());
        inventoryService.releaseStock(
                cmd.getSagaId(),
                cmd.getReservationId(),
                cmd.getReason()
        );
    }
}
