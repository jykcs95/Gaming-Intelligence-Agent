package com.gamingintel.processor_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "steam_events")
public class SteamEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "gid", nullable = false, unique = true)
    private String gid;

    @Column(name = "app_id", nullable = false)
    private Integer appId;

    @Column(name = "title")
    private String title;

    @Column(name = "event_time")
    private Instant eventTime;

    @Column(name = "raw_payload", columnDefinition = "jsonb")
    private String rawPayload;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    public SteamEventEntity() {
    }

    public SteamEventEntity(String gid, Integer appId, String title, Instant eventTime, String rawPayload) {
        this.gid = gid;
        this.appId = appId;
        this.title = title;
        this.eventTime = eventTime;
        this.rawPayload = rawPayload;
    }

    public Long getId() {
        return id;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}