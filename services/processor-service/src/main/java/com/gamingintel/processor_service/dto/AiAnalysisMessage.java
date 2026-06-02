package com.gamingintel.processor_service.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AiAnalysisMessage {

    private String gid;

    private String summary;

    private String sentiment;

    private Double confidence;

    @JsonProperty("importanceScore")
    @JsonAlias("importance_score")
    private Integer importanceScore;

    @JsonProperty("updateType")
    @JsonAlias("update_type")
    private String updateType;

    @JsonProperty("keyPoints")
    @JsonAlias("key_points")
    private List<String> keyPoints;

    @JsonProperty("createdAt")
    @JsonAlias("created_at")
    private Instant createdAt;

    private String source;
}