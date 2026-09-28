package com.saga.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.inventory.entity.OutboxEvent;
import com.saga.inventory.entity.Product;
import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.entity.enums.ReservationStatus;
import com.saga.inventory.outbox.OutboxEventRecorder;
import com.saga.inventory.repository.OutboxEventRepository;
import com.saga.inventory.repository.ProductRepository;
import com.saga.inventory.repository.StockReservationRepository;
import com.saga.shared.event.StockReleasedEvent;
import com.saga.shared.kafka.KafkaTopics;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private StockReservationRepository reservationRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private OutboxEventRecorder outboxEventRecorder;

    private InventoryService service() {
        return new InventoryService(productRepository, reservationRepository, outboxEventRepository,
                outboxEventRecorder, new ObjectMapper().findAndRegisterModules());
    }

    @Test
    void releasesReservedStockOnlyOnceAndEmitsOneEvent() throws Exception {
        UUID sagaId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        Product product = Product.builder()
                .id(productId)
                .name("Keyboard")
                .quantity(20)
                .reservedQuantity(7)
                .build();
        StockReservation reservation = StockReservation.builder()
                .id(reservationId)
                .orderId(orderId.toString())
                .product(product)
                .quantity(3)
                .status(ReservationStatus.RESERVED)
                .build();

        when(reservationRepository.findByIdWithLock(reservationId)).thenReturn(Optional.of(reservation));
        when(productRepository.findByIdWithLock(productId)).thenReturn(Optional.of(product));
        when(reservationRepository.save(reservation)).thenReturn(reservation);

        StockReservation firstResult = service().releaseStock(sagaId, reservationId, "order cancelled");
        StockReservation replayResult = service().releaseStock(sagaId, reservationId, "duplicate delivery");

        assertSame(reservation, firstResult);
        assertSame(reservation, replayResult);
        assertEquals(4, product.getReservedQuantity());
        assertEquals(ReservationStatus.RELEASED, reservation.getStatus());
        verify(productRepository, times(1)).save(product);
        verify(reservationRepository, times(1)).save(reservation);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository, times(1)).save(outboxCaptor.capture());
        OutboxEvent outboxEvent = outboxCaptor.getValue();
        assertEquals(KafkaTopics.STOCK_RELEASED, outboxEvent.getTopic());
        assertEquals(StockReleasedEvent.class.getName(), outboxEvent.getEventType());
        assertEquals(reservationId.toString(), outboxEvent.getAggregateId());
        assertEquals(reservationId.toString(),
                new ObjectMapper().readTree(outboxEvent.getPayload()).get("reservationId").asText());
    }

    @Test
    void releasedReservationIsReturnedWithoutPersistentSideEffects() {
        UUID reservationId = UUID.randomUUID();
        Product product = Product.builder()
                .id(UUID.randomUUID())
                .name("Mouse")
                .quantity(10)
                .reservedQuantity(2)
                .build();
        StockReservation reservation = StockReservation.builder()
                .id(reservationId)
                .orderId(UUID.randomUUID().toString())
                .product(product)
                .quantity(2)
                .status(ReservationStatus.RELEASED)
                .build();
        when(reservationRepository.findByIdWithLock(reservationId)).thenReturn(Optional.of(reservation));

        StockReservation result = service().releaseStock(UUID.randomUUID(), reservationId, "replay");

        assertSame(reservation, result);
        assertEquals(2, product.getReservedQuantity());
        verify(reservationRepository, never()).save(any());
        verifyNoInteractions(productRepository, outboxEventRepository);
    }

    @Test
    void missingReservationKeepsEntityNotFoundBehavior() {
        UUID reservationId = UUID.randomUUID();
        when(reservationRepository.findByIdWithLock(reservationId)).thenReturn(Optional.empty());

        EntityNotFoundException exception = assertThrows(EntityNotFoundException.class,
                () -> service().releaseStock(UUID.randomUUID(), reservationId, "cancelled"));

        assertEquals("Reservation not found: " + reservationId, exception.getMessage());
        verifyNoInteractions(productRepository, outboxEventRepository);
        verify(reservationRepository, never()).save(any());
    }
}
