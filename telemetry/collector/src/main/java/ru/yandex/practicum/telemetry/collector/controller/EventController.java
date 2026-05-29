package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.model.event.*;
import ru.yandex.practicum.telemetry.collector.service.EventService;

import java.time.Instant;
import java.util.List;

@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final EventService eventService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            eventService.handleSensorEvent(toSensorEvent(request));
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            eventService.handleHubEvent(toHubEvent(request));
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)));
        }
    }

    private HubEvent toHubEvent(HubEventProto request) {
        HubEvent event = switch (request.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent e = new DeviceAddedEvent();
                e.setId(request.getDeviceAdded().getId());
                e.setDeviceType(DeviceType.valueOf(request.getDeviceAdded().getType().name()));
                yield e;
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent e = new DeviceRemovedEvent();
                e.setId(request.getDeviceRemoved().getId());
                yield e;
            }
            case SCENARIO_ADDED -> {
                List<ScenarioCondition> conditions = request.getScenarioAdded().getConditionsList().stream()
                        .map(c -> {
                            ScenarioCondition condition = new ScenarioCondition();
                            condition.setSensorId(c.getSensorId());
                            condition.setType(ConditionType.valueOf(c.getType().name()));
                            condition.setOperation(ConditionOperation.valueOf(c.getOperation().name()));
                            condition.setValue(switch (c.getValueCase()) {
                                case BOOL_VALUE -> c.getBoolValue() ? 1 : 0;
                                case INT_VALUE -> c.getIntValue();
                                default -> null;
                            });
                            return condition;
                        })
                        .toList();
                List<DeviceAction> actions = request.getScenarioAdded().getActionsList().stream()
                        .map(a -> {
                            DeviceAction action = new DeviceAction();
                            action.setSensorId(a.getSensorId());
                            action.setType(ActionType.valueOf(a.getType().name()));
                            action.setValue(a.getValue());
                            return action;
                        })
                        .toList();
                ScenarioAddedEvent e = new ScenarioAddedEvent();
                e.setName(request.getScenarioAdded().getId());
                e.setConditions(conditions);
                e.setActions(actions);
                yield e;
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent e = new ScenarioRemovedEvent();
                e.setName(request.getScenarioRemoved().getId());
                yield e;
            }
            default -> throw new IllegalArgumentException("Unknown hub event type: " + request.getPayloadCase());
        };

        event.setHubId(request.getHubId());
        event.setTimestamp(Instant.ofEpochSecond(
                request.getTimestamp().getSeconds(),
                request.getTimestamp().getNanos()));

        return event;
    }

    private SensorEvent toSensorEvent(SensorEventProto request) {
        SensorEvent event = switch (request.getPayloadCase()) {
            case MOTION_SENSOR -> {
                MotionSensorEvent e = new MotionSensorEvent();
                e.setLinkQuality(request.getMotionSensor().getLinkQuality());
                e.setMotion(request.getMotionSensor().getMotion());
                e.setVoltage(request.getMotionSensor().getVoltage());
                yield e;
            }
            case TEMPERATURE_SENSOR -> {
                TemperatureSensorEvent e = new TemperatureSensorEvent();
                e.setTemperatureC(request.getTemperatureSensor().getTemperatureC());
                e.setTemperatureF(request.getTemperatureSensor().getTemperatureF());
                yield e;
            }
            case LIGHT_SENSOR -> {
                LightSensorEvent e = new LightSensorEvent();
                e.setLinkQuality(request.getLightSensor().getLinkQuality());
                e.setLuminosity(request.getLightSensor().getLuminosity());
                yield e;
            }
            case CLIMATE_SENSOR -> {
                ClimateSensorEvent e = new ClimateSensorEvent();
                e.setTemperatureC(request.getClimateSensor().getTemperatureC());
                e.setHumidity(request.getClimateSensor().getHumidity());
                e.setCo2Level(request.getClimateSensor().getCo2Level());
                yield e;
            }
            case SWITCH_SENSOR -> {
                SwitchSensorEvent e = new SwitchSensorEvent();
                e.setState(request.getSwitchSensor().getState());
                yield e;
            }
            default -> throw new IllegalArgumentException("Unknown sensor event type: " + request.getPayloadCase());
        };

        event.setId(request.getId());
        event.setHubId(request.getHubId());
        event.setTimestamp(Instant.ofEpochSecond(
                request.getTimestamp().getSeconds(),
                request.getTimestamp().getNanos()));

        return event;
    }
}
