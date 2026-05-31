package com.gamingintel.processor_service.repository;

import com.gamingintel.processor_service.entity.SteamEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SteamEventRepository extends JpaRepository<SteamEventEntity, Long> {

    Optional<SteamEventEntity> findByGid(String gid);
}