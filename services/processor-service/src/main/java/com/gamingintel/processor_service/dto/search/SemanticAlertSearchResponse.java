package com.gamingintel.processor_service.dto.search;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class SemanticAlertSearchResponse {

    private String gid;
    private Integer appId;
    private String gameName;
    private String url;
    private String severity;
    private String summary;
    private String sentiment;
    private Double confidence;
    private Integer importanceScore;
    private String updateType;
    private String source;
    private Instant createdAt;
    private Double similarity;
}