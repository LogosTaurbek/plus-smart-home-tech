package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.event.*;

import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventServiceImpl implements EventService {
    private final KafkaEventProducer kafkaEventProducer;

    @Override
    public void handleSensorEvent(SensorEvent event) {
        SensorEventAvro avroEvent = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapSensorPayload(event))
                .build();

        kafkaEventProducer.sendSensorEvent(avroEvent);
    }

    @Override
    public void handleHubEvent(HubEvent event) {
        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapHubPayload(event))
                .build();

        kafkaEventProducer.sendHubEvent(avroEvent);
    }

    private Object mapSensorPayload(SensorEvent event) {
        if (event instanceof ClimateSensorEvent e) {
            return ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();
        } else if (event instanceof LightSensorEvent e) {
            return LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build();
        } else if (event instanceof MotionSensorEvent e) {
            return MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.isMotion())
                    .setVoltage(e.getVoltage())
                    .build();
        } else if (event instanceof SwitchSensorEvent e) {
            return SwitchSensorAvro.newBuilder()
                    .setState(e.isState())
                    .build();
        } else if (event instanceof TemperatureSensorEvent e) {
            return TemperatureSensorAvro.newBuilder()
                    .setId(e.getId())
                    .setHubId(e.getHubId())
                    .setTimestamp(e.getTimestamp())
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build();
        }
        throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass());
    }

    private Object mapHubPayload(HubEvent event) {
        if (event instanceof DeviceAddedEvent e) {
            return DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                    .build();
        } else if (event instanceof DeviceRemovedEvent e) {
            return DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();
        } else if (event instanceof ScenarioAddedEvent e) {
            return ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(e.getConditions().stream()
                            .map(c -> ScenarioConditionAvro.newBuilder()
                                    .setSensorId(c.getSensorId())
                                    .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                    .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                    .setValue(c.getValue())
                                    .build())
                            .collect(Collectors.toList()))
                    .setActions(e.getActions().stream()
                            .map(a -> DeviceActionAvro.newBuilder()
                                    .setSensorId(a.getSensorId())
                                    .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                    .setValue(a.getValue())
                                    .build())
                            .collect(Collectors.toList()))
                    .build();
        } else if (event instanceof ScenarioRemovedEvent e) {
            return ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();
        }
        throw new IllegalArgumentException("Unknown hub event type: " + event.getClass());
    }
}
