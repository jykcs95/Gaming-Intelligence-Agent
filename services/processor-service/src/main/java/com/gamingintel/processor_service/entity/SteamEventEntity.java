package com.gamingintel.processor_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "steam_events")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SteamEventEntity {

    @Id
    @Column(name = "gid", nullable = false)
    private String gid;

    @Column(name = "app_id", nullable = false)
    private Integer appId;

    @Column(name = "title")
    private String title;

    @Column(name = "event_time")
    private Instant eventTime;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw_payload", columnDefinition = "jsonb")
    private String rawPayload;
}