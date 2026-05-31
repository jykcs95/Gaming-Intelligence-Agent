package com.gamingintel.processor_service.repository;

import com.gamingintel.processor_service.entity.AlertEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AlertRepository extends JpaRepository<AlertEntity, Long> {

    Optional<AlertEntity> findByAlertId(String alertId);

    boolean existsByAlertId(String alertId);

    Optional<AlertEntity> findByGid(String gid);

    boolean existsByGid(String gid);
}