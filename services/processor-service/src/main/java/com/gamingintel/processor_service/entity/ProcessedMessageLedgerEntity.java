package com.gamingintel.processor_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processed_message_ledger")
public class ProcessedMessageLedgerEntity {

    @Id
    @Column(name = "gid", nullable = false)
    private String gid;

    @Column(name = "processed_at")
    private Instant processedAt = Instant.now();

    public ProcessedMessageLedgerEntity() {
    }

    public ProcessedMessageLedgerEntity(String gid) {
        this.gid = gid;
    }

    public String getGid() {
        return gid;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}