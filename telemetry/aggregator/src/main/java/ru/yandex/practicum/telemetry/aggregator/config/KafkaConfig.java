package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaConfig {
    private String bootstrapServers;
    private ConsumerConfig consumer;
    private ProducerConfig producer;

    @Getter
    @Setter
    public static class ConsumerConfig {
        private String groupId;
        private String sensorsTopic;
    }

    @Getter
    @Setter
    public static class ProducerConfig {
        private String snapshotsTopic;
    }
}
