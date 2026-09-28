package com.saga.inventory.outbox;

import com.saga.inventory.entity.OutboxEvent;
import com.saga.inventory.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

// Separate bean (not a method on InventoryService) so REQUIRES_NEW actually applies —
// a self-invoked call from within the same class would bypass the Spring proxy and
// silently run in the caller's existing transaction instead.
@Service
@RequiredArgsConstructor
public class OutboxEventRecorder {

    private final OutboxEventRepository outboxEventRepository;

    // Used when the outbox row must survive a rollback the caller triggers right after
    // recording it (e.g. a business-failure exception thrown to signal callers upstream).
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordInNewTransaction(OutboxEvent event) {
        outboxEventRepository.save(event);
    }
}
