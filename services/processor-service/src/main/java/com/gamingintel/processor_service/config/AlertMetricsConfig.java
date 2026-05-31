package com.gamingintel.processor_service.config;

import com.gamingintel.processor_service.repository.AlertRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AlertMetricsConfig {

        public AlertMetricsConfig(MeterRegistry meterRegistry, AlertRepository alertRepository) {

                String databaseSeverity = "alerts_database_by_severity";
                String severity = "severity";

                Gauge.builder("alerts_database_total", alertRepository, AlertRepository::count)
                                .description("Current total number of alerts stored in the database")
                                .register(meterRegistry);

                Gauge.builder(databaseSeverity, alertRepository,
                                repository -> repository.countBySeverityIgnoreCase("critical"))
                                .description("Current number of critical alerts stored in the database")
                                .tag(severity, "critical")
                                .register(meterRegistry);

                Gauge.builder(databaseSeverity, alertRepository,
                                repository -> repository.countBySeverityIgnoreCase("high"))
                                .description("Current number of high alerts stored in the database")
                                .tag(severity, "high")
                                .register(meterRegistry);

                Gauge.builder(databaseSeverity, alertRepository,
                                repository -> repository.countBySeverityIgnoreCase("medium"))
                                .description("Current number of medium alerts stored in the database")
                                .tag(severity, "medium")
                                .register(meterRegistry);

                Gauge.builder(databaseSeverity, alertRepository,
                                repository -> repository.countBySeverityIgnoreCase("low"))
                                .description("Current number of low alerts stored in the database")
                                .tag(severity, "low")
                                .register(meterRegistry);
        }
}