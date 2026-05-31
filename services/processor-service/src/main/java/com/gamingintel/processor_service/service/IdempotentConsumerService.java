package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.entity.ProcessedMessageLedgerEntity;
import com.gamingintel.processor_service.repository.ProcessedMessageLedgerRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdempotentConsumerService {

    private final ProcessedMessageLedgerRepository ledgerRepository;

    public IdempotentConsumerService(
            ProcessedMessageLedgerRepository ledgerRepository) {
        this.ledgerRepository = ledgerRepository;
    }

    @Transactional(readOnly = true)
    public boolean alreadyProcessed(String gid) {
        return ledgerRepository.existsById(gid);
    }

    @Transactional
    public void markProcessed(String gid) {
        ledgerRepository.save(new ProcessedMessageLedgerEntity(gid));
    }
}