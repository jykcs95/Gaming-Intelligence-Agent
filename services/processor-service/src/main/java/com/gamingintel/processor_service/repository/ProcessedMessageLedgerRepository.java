package com.gamingintel.processor_service.repository;

import com.gamingintel.processor_service.entity.ProcessedMessageLedgerEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageLedgerRepository
        extends JpaRepository<ProcessedMessageLedgerEntity, String> {
}