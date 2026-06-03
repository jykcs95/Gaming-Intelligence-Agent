package com.gamingintel.processor_service.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SteamUpdateMessage {

    @NotBlank
    private String gid;

    @NotNull
    @JsonProperty("app_id")
    private Integer appId;

    private String title;

    private String url;

    private String author;

    private String contents;

    private Long date;

    @JsonProperty("game_name")
    @JsonAlias("gameName")
    private String gameName;

    @JsonProperty("alert_keywords")
    @JsonAlias("alertKeywords")
    private List<String> alertKeywords;

    @JsonProperty("published_at")
    private String publishedAt;
}