package com.gamingintel.processor_service.repository.search;

import com.gamingintel.processor_service.dto.search.SemanticAlertSearchResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class SemanticSearchRepository {

    private final JdbcTemplate jdbcTemplate;

    public void deleteAlertEmbeddings() {
        jdbcTemplate.update("DELETE FROM embeddings WHERE source_type = 'alert'");
    }

    public void insertAlertEmbedding(String gid, String sourceText, List<Double> embedding) {
        String sql = """
                INSERT INTO embeddings (gid, source_type, source_text, embedding, created_at)
                VALUES (?, 'alert', ?, ?::vector, NOW())
                """;

        jdbcTemplate.update(sql, gid, sourceText, toVectorLiteral(embedding));
    }

    public List<SemanticAlertSearchResponse> searchAlerts(List<Double> queryEmbedding, int limit) {
        String sql = """
                SELECT
                    a.gid,
                    a.app_id,
                    a.game_name,
                    a.url,
                    a.severity,
                    a.summary,
                    a.sentiment,
                    a.confidence,
                    a.importance_score,
                    a.update_type,
                    a.source,
                    a.created_at,
                    1 - (e.embedding <=> ?::vector) AS similarity
                FROM embeddings e
                JOIN alerts a ON a.gid = e.gid
                WHERE e.source_type = 'alert'
                ORDER BY e.embedding <=> ?::vector
                LIMIT ?
                """;

        String vectorLiteral = toVectorLiteral(queryEmbedding);

        return jdbcTemplate.query(
                sql,
                ps -> {
                    ps.setString(1, vectorLiteral);
                    ps.setString(2, vectorLiteral);
                    ps.setInt(3, limit);
                },
                (rs, rowNum) -> {
                    Timestamp createdAt = rs.getTimestamp("created_at");
                    Instant createdAtInstant = createdAt == null ? null : createdAt.toInstant();

                    return SemanticAlertSearchResponse.builder()
                            .gid(rs.getString("gid"))
                            .appId((Integer) rs.getObject("app_id"))
                            .gameName(rs.getString("game_name"))
                            .url(rs.getString("url"))
                            .severity(rs.getString("severity"))
                            .summary(rs.getString("summary"))
                            .sentiment(rs.getString("sentiment"))
                            .confidence((Double) rs.getObject("confidence"))
                            .importanceScore((Integer) rs.getObject("importance_score"))
                            .updateType(rs.getString("update_type"))
                            .source(rs.getString("source"))
                            .createdAt(createdAtInstant)
                            .similarity(rs.getDouble("similarity"))
                            .build();
                });
    }

    private String toVectorLiteral(List<Double> vector) {
        return "[" + vector.stream()
                .map(String::valueOf)
                .reduce((left, right) -> left + "," + right)
                .orElse("") + "]";
    }
}