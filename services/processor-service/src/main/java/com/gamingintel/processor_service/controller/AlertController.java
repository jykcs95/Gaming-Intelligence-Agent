package com.gamingintel.processor_service.controller;

import com.gamingintel.processor_service.dto.AlertResponse;
import com.gamingintel.processor_service.dto.AlertSummaryResponse;
import com.gamingintel.processor_service.service.AlertQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertQueryService alertQueryService;

    public AlertController(AlertQueryService alertQueryService) {
        this.alertQueryService = alertQueryService;
    }

    @GetMapping
    public ResponseEntity<List<AlertResponse>> getAlerts(
            @RequestParam(required = false) String severity,
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(alertQueryService.getAlerts(severity, limit));
    }

    @GetMapping("/summary")
    public ResponseEntity<AlertSummaryResponse> getAlertSummary() {
        return ResponseEntity.ok(alertQueryService.getAlertSummary());
    }

    @GetMapping("/severity/{severity}")
    public ResponseEntity<List<AlertResponse>> getAlertsBySeverity(@PathVariable String severity) {
        return ResponseEntity.ok(alertQueryService.getAlertsBySeverity(severity));
    }

    @GetMapping("/{gid}")
    public ResponseEntity<AlertResponse> getAlertByGid(@PathVariable String gid) {
        return ResponseEntity.ok(alertQueryService.getAlertByGid(gid));
    }
}