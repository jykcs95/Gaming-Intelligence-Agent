package com.gamingintel.processor_service.dto.search;

import lombok.Data;

import java.util.List;

@Data
public class OllamaEmbedResponse {

    private String model;
    private List<List<Double>> embeddings;
}