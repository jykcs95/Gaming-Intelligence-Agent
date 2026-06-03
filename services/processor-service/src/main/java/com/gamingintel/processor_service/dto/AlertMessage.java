package com.gamingintel.processor_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertMessage {

    private String alertId;

    private String gid;

    private String severity;

    private Integer importanceScore;

    private String sentiment;

    private Double confidence;

    private String updateType;

    private String summary;

    @JsonProperty("appId")
    @JsonAlias("app_id")
    private Integer appId;

    @JsonProperty("gameName")
    @JsonAlias("game_name")
    private String gameName;

    private String url;

    @JsonProperty("triggeredRules")
    @JsonAlias("triggered_rules")
    private List<String> triggeredRules;

    private String createdAt;

    private String source;
}