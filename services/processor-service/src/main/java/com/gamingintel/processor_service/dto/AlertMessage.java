package com.gamingintel.processor_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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

    private List<String> triggeredRules;

    private String createdAt;

    private String source;
}