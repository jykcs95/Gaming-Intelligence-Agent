package com.gamingintel.processor_service.controller;

import com.gamingintel.processor_service.dto.search.SemanticAlertSearchResponse;
import com.gamingintel.processor_service.service.SemanticSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/search")
@RequiredArgsConstructor
public class SemanticSearchController {

    private final SemanticSearchService semanticSearchService;

    @GetMapping("/alerts")
    public List<SemanticAlertSearchResponse> searchAlerts(
            @RequestParam String query,
            @RequestParam(defaultValue = "5") int limit) {
        return semanticSearchService.searchAlerts(query, limit);
    }

    @PostMapping("/alerts/rebuild-index")
    public Map<String, Object> rebuildAlertSearchIndex() {
        int indexed = semanticSearchService.rebuildAlertEmbeddings();

        return Map.of(
                "status", "ok",
                "indexedAlerts", indexed);
    }
}