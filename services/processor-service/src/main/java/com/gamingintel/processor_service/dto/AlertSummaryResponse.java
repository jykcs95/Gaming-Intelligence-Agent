package com.gamingintel.processor_service.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AlertSummaryResponse {

    private long totalAlerts;

    private long criticalCount;

    private long highCount;

    private long mediumCount;

    private long lowCount;
}