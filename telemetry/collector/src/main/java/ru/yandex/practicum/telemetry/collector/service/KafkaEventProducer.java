package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventProducer {
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    public void sendSensorEvent(SensorEventAvro event) {
        log.info("Sending sensor event to Kafka: {}", event);
        kafkaTemplate.send("telemetry.sensors.v1", event.getHubId(), event);
    }

    public void sendHubEvent(HubEventAvro event) {
        log.info("Sending hub event to Kafka: {}", event);
        kafkaTemplate.send("telemetry.hubs.v1", event.getHubId(), event);
    }
}
