package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
@Service
public class KafkaEventProducer {
    private final Producer<String, byte[]> producer;
    private final String sensorsTopic;
    private final String hubsTopic;

    public KafkaEventProducer(
            Producer<String, byte[]> producer,
            @Value("${collector.kafka.topics.sensors}") String sensorsTopic,
            @Value("${collector.kafka.topics.hubs}") String hubsTopic) {
        this.producer = producer;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void sendSensorEvent(SensorEventAvro event) {
        send(sensorsTopic, event.getHubId(), event);
    }

    public void sendHubEvent(HubEventAvro event) {
        send(hubsTopic, event.getHubId(), event);
    }

    private <T extends SpecificRecordBase> void send(String topic, String key, T avro) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<T> writer = new SpecificDatumWriter<>(avro.getSchema());
            writer.write(avro, encoder);
            encoder.flush();
            producer.send(new ProducerRecord<>(topic, key, out.toByteArray()), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending event to Kafka", exception);
                } else {
                    log.debug("Event sent to Kafka: topic={}, partition={}, offset={}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Ошибка сериализации Avro", e);
        }
    }
}
