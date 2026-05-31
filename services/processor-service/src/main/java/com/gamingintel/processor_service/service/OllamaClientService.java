package com.gamingintel.processor_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Slf4j
@Service
public class OllamaClientService {

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final String model;

    public OllamaClientService(
            RestClient.Builder restClientBuilder,
            ObjectMapper objectMapper,
            @Value("${ollama.base-url:http://[::1]:11434}") String baseUrl,
            @Value("${ollama.model:llama3.2:latest}") String model) {
        this.restClient = restClientBuilder
                .baseUrl(baseUrl)
                .build();
        this.objectMapper = objectMapper;
        this.model = model.trim();
    }

    public String analyzeSteamUpdate(ProcessedSteamUpdateMessage message) {
        String prompt = buildPrompt(message);

        Map<String, Object> requestBody = Map.of(
                "model", this.model,
                "prompt", prompt,
                "stream", false,
                "format", "json",
                "options", Map.of(
                        "temperature", 0));

        log.info("Calling Ollama model={}", model);

        String response = restClient.post()
                .uri("/api/generate")
                .body(requestBody)
                .retrieve()
                .body(String.class);

        log.info("Received raw Ollama response for gid={} response={}", message.getGid(), response);

        return extractResponse(response);
    }

    private String buildPrompt(ProcessedSteamUpdateMessage message) {
        return """
                You are a gaming intelligence analyst.

                Analyze this Steam update. Return ONLY one valid JSON object. Do not write any natural language before or after it. Do not apologize. Do not explain.:
                {
                    "summary": "short summary of the update",
                    "sentiment": "positive, neutral, or negative",
                    "confidence": 0.0,
                    "importanceScore": 1,
                    "updateType": "patch",
                    "keyPoints": "short key point 1; short key point 2"
                }

                Rules:
                - sentiment must be one of: positive, neutral, negative
                - confidence must be a number from 0.0 to 1.0
                - importanceScore must be an integer from 1 to 10
                - updateType must be one of:
                    patch, event, announcement, balance_change, dlc, sale, bug_fix, security, unknown
                - keyPoints must be a semicolon-separated string, not an array
                - Return only valid JSON
                - Do not include markdown
                - Do not include explanation outside the JSON
                - The first character of your response must be {
                - The last character of your response must be }
                - If the update mentions security, exploit, vulnerability, account safety, cheating, or player safety, updateType must be security and importanceScore must be at least 8.

                Steam update:
                App ID: %s
                Title: %s
                Author: %s
                URL: %s
                Contents:
                %s
                """
                .formatted(
                        message.getAppId(),
                        message.getTitle(),
                        message.getAuthor(),
                        message.getUrl(),
                        message.getContents());
    }

    private String extractResponse(String rawResponse) {
        try {
            JsonNode root = objectMapper.readTree(rawResponse);
            String responseText = root.path("response").asText();

            return extractJsonObject(responseText);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse Ollama response", ex);
        }
    }

    private String extractJsonObject(String responseText) {
        if (responseText == null || responseText.isBlank()) {
            throw new IllegalArgumentException("Ollama response was empty");
        }

        String cleaned = responseText
                .replace("```json", "")
                .replace("```", "")
                .trim();

        int start = cleaned.indexOf('{');
        int end = cleaned.lastIndexOf('}');

        if (start < 0 || end < 0 || end <= start) {
            throw new IllegalArgumentException(
                    "Ollama response did not contain a valid JSON object: " + responseText);
        }

        return cleaned.substring(start, end + 1);
    }
}