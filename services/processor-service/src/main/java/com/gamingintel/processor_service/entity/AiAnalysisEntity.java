package com.gamingintel.processor_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "ai_analysis")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AiAnalysisEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "gid", nullable = false)
    private String gid;

    @Column(name = "summary")
    private String summary;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "confidence")
    private Double confidence;

    @Column(name = "importance_score")
    private Integer importanceScore;

    @Column(name = "update_type")
    private String updateType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "key_points", columnDefinition = "jsonb")
    private List<String> keyPoints;

    @Column(name = "created_at")
    private Instant createdAt;
}