package com.gamingintel.processor_service.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AiAnalysisMessage {

    private String gid;

    private String summary;

    private String sentiment;

    private Double confidence;

    private Integer importanceScore;

    private String updateType;

    private String keyPoints;

    private String createdAt;

    private String source;
}