package com.saga.orchestrator.repository;

import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.enums.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByOrderId(String orderId);

    Optional<SagaInstance> findByIdempotencyKey(String idempotencyKey);

    List<SagaInstance> findByStatus(SagaStatus status);

    // FOR UPDATE SKIP LOCKED lets multiple orchestrator instances claim stuck sagas
    // concurrently without blocking each other on the same row.
    @Query(value = """
            SELECT * FROM saga_instances
            WHERE status IN ('STARTED', 'COMPENSATING') AND updated_at <= :staleBefore
            ORDER BY updated_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<SagaInstance> claimNextStuck(@Param("staleBefore") LocalDateTime staleBefore);
}
