package com.gamingintel.processor_service.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@Builder
public class AlertResponse {

    private String alertId;

    private String gid;

    private String severity;

    private Integer importanceScore;

    private String sentiment;

    private Double confidence;

    private String updateType;

    private String summary;

    private Integer appId;

    private String gameName;

    private String url;

    @JsonProperty("triggeredRules")
    @JsonAlias("triggered_rules")
    private List<String> triggeredRules;

    private String source;

    private Instant createdAt;
}