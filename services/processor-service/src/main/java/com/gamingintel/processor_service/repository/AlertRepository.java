package com.gamingintel.processor_service.repository;

import com.gamingintel.processor_service.entity.AlertEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.Collection;

public interface AlertRepository extends JpaRepository<AlertEntity, Long> {

    Optional<AlertEntity> findByAlertId(String alertId);

    boolean existsByAlertId(String alertId);

    Optional<AlertEntity> findByGid(String gid);

    boolean existsByGid(String gid);

    List<AlertEntity> findTop20ByOrderByCreatedAtDesc();

    List<AlertEntity> findByOrderByCreatedAtDesc(Pageable pageable);

    List<AlertEntity> findBySeverityIgnoreCaseOrderByCreatedAtDesc(String severity, Pageable pageable);

    List<AlertEntity> findBySeverityInIgnoreCaseOrderByCreatedAtDesc(
            Collection<String> severities,
            Pageable pageable);

    long countBySeverityIgnoreCase(String severity);
}