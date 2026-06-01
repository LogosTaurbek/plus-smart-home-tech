package ru.yandex.practicum.telemetry.analyzer.handler;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final ScenarioRepository scenarioRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Value("${analyzer.kafka.topics.snapshots}")
    private String snapshotsTopic;

    public void start() {
        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("SnapshotProcessor subscribed to topic: {}", snapshotsTopic);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record.value());
                }

                if (!records.isEmpty()) {
                    snapshotConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            snapshotConsumer.close();
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) return;

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        for (Scenario scenario : scenarios) {
            boolean allConditionsMet = scenario.getConditions().stream()
                    .allMatch(sc -> checkCondition(sc, sensorsState));

            if (allConditionsMet && !scenario.getConditions().isEmpty()) {
                executeActions(scenario, timestamp);
            }
        }
    }

    private boolean checkCondition(ScenarioCondition sc, Map<String, SensorStateAvro> sensorsState) {
        String sensorId = sc.getSensor().getId();
        SensorStateAvro state = sensorsState.get(sensorId);
        if (state == null) return false;

        Condition condition = sc.getCondition();
        Integer threshold = condition.getValue();
        if (threshold == null) return false;

        Integer sensorValue = extractSensorValue(state.getData(), condition.getType());
        if (sensorValue == null) return false;

        return switch (condition.getOperation()) {
            case "EQUALS" -> sensorValue.equals(threshold);
            case "GREATER_THAN" -> sensorValue > threshold;
            case "LOWER_THAN" -> sensorValue < threshold;
            default -> false;
        };
    }

    private Integer extractSensorValue(Object payload, String conditionType) {
        return switch (payload) {
            case ClimateSensorAvro climate -> switch (conditionType) {
                case "TEMPERATURE" -> climate.getTemperatureC();
                case "CO2LEVEL" -> climate.getCo2Level();
                case "HUMIDITY" -> climate.getHumidity();
                default -> null;
            };
            case LightSensorAvro light -> switch (conditionType) {
                case "LUMINOSITY" -> light.getLuminosity();
                default -> null;
            };
            case MotionSensorAvro motion -> switch (conditionType) {
                case "MOTION" -> motion.getMotion() ? 1 : 0;
                default -> null;
            };
            case SwitchSensorAvro sw -> switch (conditionType) {
                case "SWITCH" -> sw.getState() ? 1 : 0;
                default -> null;
            };
            case TemperatureSensorAvro temp -> switch (conditionType) {
                case "TEMPERATURE" -> temp.getTemperatureC();
                default -> null;
            };
            case null, default -> null;
        };
    }

    private void executeActions(Scenario scenario, Timestamp timestamp) {
        for (ScenarioAction sa : scenario.getActions()) {
            try {
                Action action = sa.getAction();
                DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                        .setSensorId(sa.getSensor().getId())
                        .setType(ActionTypeProto.valueOf(action.getType()));

                if (action.getValue() != null) {
                    actionBuilder.setValue(action.getValue());
                }

                DeviceActionRequest request = DeviceActionRequest.newBuilder()
                        .setHubId(scenario.getHubId())
                        .setScenarioName(scenario.getName())
                        .setAction(actionBuilder.build())
                        .setTimestamp(timestamp)
                        .build();

                hubRouterClient.handleDeviceAction(request);
                log.debug("Executed action {} for scenario {} hub {}",
                        action.getType(), scenario.getName(), scenario.getHubId());
            } catch (Exception e) {
                log.error("Failed to execute action for scenario {}", scenario.getName(), e);
            }
        }
    }
}
