package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.entity.SteamEventEntity;
import com.gamingintel.processor_service.repository.SteamEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class SteamEventService {

    private final SteamEventRepository steamEventRepository;

    public SteamEventService(
            SteamEventRepository steamEventRepository) {
        this.steamEventRepository = steamEventRepository;
    }

    @Transactional
    public SteamEventEntity saveEvent(
            String gid,
            Integer appId,
            String title,
            Instant eventTime,
            String rawPayload) {
        SteamEventEntity event = new SteamEventEntity(
                gid,
                appId,
                title,
                eventTime,
                rawPayload);

        return steamEventRepository.save(event);
    }
}