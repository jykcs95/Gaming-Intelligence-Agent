package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.search.OllamaEmbedRequest;
import com.gamingintel.processor_service.dto.search.OllamaEmbedResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OllamaEmbeddingClient {

    private final RestClient restClient;

    @Value("${app.embeddings.ollama-url:http://localhost:11434}")
    private String ollamaUrl;

    @Value("${app.embeddings.model:all-minilm}")
    private String embeddingModel;

    public List<Double> embed(String input) {
        OllamaEmbedRequest request = OllamaEmbedRequest.builder()
                .model(embeddingModel)
                .input(input)
                .build();

        OllamaEmbedResponse response = restClient.post()
                .uri(ollamaUrl + "/api/embed")
                .body(request)
                .retrieve()
                .body(OllamaEmbedResponse.class);

        if (response == null || response.getEmbeddings() == null || response.getEmbeddings().isEmpty()) {
            throw new IllegalStateException("Ollama embedding response did not contain embeddings");
        }

        return response.getEmbeddings().get(0);
    }
}