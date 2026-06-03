package com.gamingintel.processor_service.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedSteamUpdateMessage {

    private String gid;

    @JsonProperty("app_id")
    private Integer appId;

    private String title;

    private String url;

    private String author;

    private String contents;

    @JsonProperty("gameName")
    @JsonAlias("game_name")
    private String gameName;

    @JsonProperty("alertKeywords")
    @JsonAlias("alert_keywords")
    private List<String> alertKeywords;

    @JsonProperty("event_time")
    private String eventTime;

    @JsonProperty("processed_at")
    private String processedAt;

    private String source;
}