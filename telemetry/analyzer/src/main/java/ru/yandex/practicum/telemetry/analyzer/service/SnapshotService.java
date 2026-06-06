package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService {

    private final ScenarioRepository scenarioRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Transactional(readOnly = true)
    public void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("No scenarios for hub: {}", hubId);
            return;
        }

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        for (Scenario scenario : scenarios) {
            log.info("Checking scenario [{}] for hub {}", scenario.getName(), hubId);
            if (scenario.getConditions().isEmpty()) {
                log.warn("Scenario [{}] has no conditions!", scenario.getName());
                continue;
            }

            boolean allConditionsMet = scenario.getConditions().stream()
                    .allMatch(sc -> {
                        boolean met = checkCondition(sc, sensorsState);
                        log.debug("Condition for sensor {} met: {}", sc.getSensor().getId(), met);
                        return met;
                    });

            if (allConditionsMet) {
                log.info("Scenario [{}] conditions met for hub {}", scenario.getName(), hubId);
                executeActions(scenario, timestamp);
            } else {
                log.info("Scenario [{}] conditions NOT met for hub {}", scenario.getName(), hubId);
            }
        }
    }

    private boolean checkCondition(ScenarioCondition sc, Map<String, SensorStateAvro> sensorsState) {
        String sensorId = sc.getSensor().getId();
        SensorStateAvro state = sensorsState.get(sensorId);
        if (state == null) return false;

        Condition condition = sc.getCondition();
        Integer sensorValue = extractSensorValue(state.getData(), condition.getType());
        if (sensorValue == null) return false;

        Integer threshold = condition.getValue();

        if (threshold == null) {
            return sensorValue != 0;
        }

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
                log.info("Sent action {} to hub {} for scenario {}",
                        action.getType(), scenario.getHubId(), scenario.getName());
            } catch (Exception e) {
                log.error("Failed to execute action for scenario {}", scenario.getName(), e);
            }
        }
    }
}
