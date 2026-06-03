package com.gamingintel.processor_service.dto.search;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OllamaEmbedRequest {

    private String model;
    private String input;
}