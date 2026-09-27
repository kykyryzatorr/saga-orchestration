package com.saga.orchestrator.entity;

import com.saga.orchestrator.entity.enums.SagaStatus;
import com.saga.orchestrator.entity.enums.SagaStepName;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "saga_instances")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString(exclude = "steps")
public class SagaInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false, length = 100)
    private SagaStepName currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    @Builder.Default
    private SagaStatus status = SagaStatus.STARTED;

    // JSON payload storing context data (userId, productId, unitPrice, totalAmount, etc.)
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(name = "request_hash", length = 64)
    private String requestHash;

    @Column(name = "recovery_attempts", nullable = false)
    @Builder.Default
    private Integer recoveryAttempts = 0;

    @OneToMany(mappedBy = "sagaInstance", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Builder.Default
    private List<SagaStep> steps = new ArrayList<>();

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
