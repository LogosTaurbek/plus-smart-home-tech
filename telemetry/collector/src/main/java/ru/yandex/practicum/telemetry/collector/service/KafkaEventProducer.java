package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
public class KafkaEventProducer {
    private final Producer<String, SpecificRecordBase> producer;
    private final String sensorsTopic;
    private final String hubsTopic;

    public KafkaEventProducer(
            Producer<String, SpecificRecordBase> producer,
            @Value("${collector.kafka.topics.sensors}") String sensorsTopic,
            @Value("${collector.kafka.topics.hubs}") String hubsTopic) {
        this.producer = producer;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void sendSensorEvent(SensorEventAvro event) {
        log.info("Sending sensor event to Kafka: topic={}, key={}, event={}", sensorsTopic, event.getHubId(), event);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(sensorsTopic, event.getHubId(), event);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending sensor event to Kafka", exception);
            } else {
                log.debug("Sensor event sent to Kafka: topic={}, partition={}, offset={}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void sendHubEvent(HubEventAvro event) {
        log.info("Sending hub event to Kafka: topic={}, key={}, event={}", hubsTopic, event.getHubId(), event);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(hubsTopic, event.getHubId(), event);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending hub event to Kafka", exception);
            } else {
                log.debug("Hub event sent to Kafka: topic={}, partition={}, offset={}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
}
