package com.saga.inventory.repository;

import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.entity.enums.ReservationStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface StockReservationRepository extends JpaRepository<StockReservation, UUID> {

    Optional<StockReservation> findByOrderId(String orderId);

    Optional<StockReservation> findByIdempotencyKey(String idempotencyKey);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select reservation from StockReservation reservation where reservation.id = :id")
    Optional<StockReservation> findByIdWithLock(@Param("id") UUID id);

    List<StockReservation> findByStatus(ReservationStatus status);
}
