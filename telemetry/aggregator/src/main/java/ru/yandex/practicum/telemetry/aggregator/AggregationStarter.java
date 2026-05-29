package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.KafkaConfig;
import ru.yandex.practicum.telemetry.aggregator.serialization.AvroSerializer;
import ru.yandex.practicum.telemetry.aggregator.serialization.HubEventDeserializer;
import ru.yandex.practicum.telemetry.aggregator.serialization.SensorEventDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final KafkaConfig kafkaConfig;
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public void start() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getConsumer().getGroupId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Properties producerProps = new Properties();
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);

        SensorEventDeserializer sensorEventDeserializer = new SensorEventDeserializer();
        HubEventDeserializer hubEventDeserializer = new HubEventDeserializer();

        try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, SensorsSnapshotAvro> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Arrays.asList(
                    kafkaConfig.getConsumer().getSensorsTopic(),
                    kafkaConfig.getConsumer().getHubsTopic()
            ));
            log.info("Subscribed to topics: {}, {}",
                    kafkaConfig.getConsumer().getSensorsTopic(),
                    kafkaConfig.getConsumer().getHubsTopic());

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        if (record.topic().equals(kafkaConfig.getConsumer().getSensorsTopic())) {
                            SensorEventAvro event = sensorEventDeserializer.deserialize(record.topic(), record.value());
                            updateState(event).ifPresent(snapshot -> sendSnapshot(producer, snapshot));
                        } else if (record.topic().equals(kafkaConfig.getConsumer().getHubsTopic())) {
                            HubEventAvro event = hubEventDeserializer.deserialize(record.topic(), record.value());
                            handleHubEvent(event).ifPresent(snapshot -> sendSnapshot(producer, snapshot));
                        }
                    } catch (Exception e) {
                        log.error("Error processing record from topic {}", record.topic(), e);
                    }
                }
                producer.flush();
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            // ignore
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        }
    }

    private void sendSnapshot(Producer<String, SensorsSnapshotAvro> producer, SensorsSnapshotAvro snapshot) {
        producer.send(new ProducerRecord<>(
                kafkaConfig.getProducer().getSnapshotsTopic(),
                snapshot.getHubId(),
                snapshot
        ), (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending snapshot for hub {} to Kafka", snapshot.getHubId(), exception);
            } else {
                log.info("Sent snapshot for hub: {} to topic: {} partition: {} offset: {}",
                        snapshot.getHubId(), metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId().toString();
        SensorsSnapshotAvro oldSnapshot = snapshots.get(hubId);
        String sensorId = event.getId().toString();

        Map<String, SensorStateAvro> stateMap = getMutableStateMap(oldSnapshot);

        if (oldSnapshot != null) {
            SensorStateAvro oldState = stateMap.get(sensorId);
            if (oldState != null) {
                if (oldState.getTimestamp().isAfter(event.getTimestamp()) ||
                        oldState.getData().equals(event.getPayload())) {
                    return Optional.empty();
                }
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        stateMap.put(sensorId, newState);

        Instant snapshotTimestamp = event.getTimestamp();
        if (oldSnapshot != null && oldSnapshot.getTimestamp().isAfter(snapshotTimestamp)) {
            snapshotTimestamp = oldSnapshot.getTimestamp();
        }

        SensorsSnapshotAvro newSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(snapshotTimestamp)
                .setSensorsState(stateMap)
                .build();

        snapshots.put(hubId, newSnapshot);
        return Optional.of(newSnapshot);
    }

    private Optional<SensorsSnapshotAvro> handleHubEvent(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        SensorsSnapshotAvro oldSnapshot = snapshots.get(hubId);

        if (event.getPayload() instanceof DeviceRemovedEventAvro) {
            DeviceRemovedEventAvro removedEvent = (DeviceRemovedEventAvro) event.getPayload();
            String sensorId = removedEvent.getId().toString();

            if (oldSnapshot != null) {
                Map<String, SensorStateAvro> stateMap = getMutableStateMap(oldSnapshot);
                if (stateMap.containsKey(sensorId)) {
                    stateMap.remove(sensorId);

                    Instant snapshotTimestamp = event.getTimestamp();
                    if (oldSnapshot.getTimestamp().isAfter(snapshotTimestamp)) {
                        snapshotTimestamp = oldSnapshot.getTimestamp();
                    }

                    SensorsSnapshotAvro newSnapshot = SensorsSnapshotAvro.newBuilder()
                            .setHubId(hubId)
                            .setTimestamp(snapshotTimestamp)
                            .setSensorsState(stateMap)
                            .build();

                    snapshots.put(hubId, newSnapshot);
                    return Optional.of(newSnapshot);
                }
            }
        }
        return Optional.empty();
    }

    private Map<String, SensorStateAvro> getMutableStateMap(SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> stateMap = new HashMap<>();
        if (snapshot != null && snapshot.getSensorsState() != null) {
            // Ensure keys are converted to String to avoid Utf8/String mismatch
            snapshot.getSensorsState().forEach((k, v) -> stateMap.put(k.toString(), v));
        }
        return stateMap;
    }
}
