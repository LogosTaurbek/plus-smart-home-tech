package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
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
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.event.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventServiceImpl implements EventService {

    private final Producer<String, byte[]> producer;

    @Value("${collector.kafka.topics.sensors}")
    private String sensorsTopic;

    @Value("${collector.kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void handleSensorEvent(SensorEvent event) {
        Object payload = switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent e = (ClimateSensorEvent) event;
                yield ClimateSensorAvro.newBuilder()
                        .setTemperatureC(e.getTemperatureC())
                        .setHumidity(e.getHumidity())
                        .setCo2Level(e.getCo2Level())
                        .build();
            }
            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent e = (LightSensorEvent) event;
                yield LightSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setLuminosity(e.getLuminosity())
                        .build();
            }
            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent e = (MotionSensorEvent) event;
                yield MotionSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setMotion(e.isMotion())
                        .setVoltage(e.getVoltage())
                        .build();
            }
            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent e = (SwitchSensorEvent) event;
                yield SwitchSensorAvro.newBuilder()
                        .setState(e.isState())
                        .build();
            }
            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent e = (TemperatureSensorEvent) event;
                yield TemperatureSensorAvro.newBuilder()
                        .setId(e.getId())
                        .setHubId(e.getHubId())
                        .setTimestamp(e.getTimestamp())
                        .setTemperatureC(e.getTemperatureC())
                        .setTemperatureF(e.getTemperatureF())
                        .build();
            }
        };

        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();

        send(sensorsTopic, event.getHubId(), avro);
    }

    @Override
    public void handleHubEvent(HubEvent event) {
        Object payload = switch (event.getType()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent e = (DeviceAddedEvent) event;
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(e.getId())
                        .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent e = (DeviceRemovedEvent) event;
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(e.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEvent e = (ScenarioAddedEvent) event;

                e.getConditions().forEach(c ->
                        System.out.println(">>> CONDITION: sensorId=" + c.getSensorId()
                                + " value=" + c.getValue()
                                + " valueType=" + (c.getValue() == null ? "null" : c.getValue().getClass().getName()))
                );
                e.getActions().forEach(a ->
                        System.out.println(">>> ACTION: sensorId=" + a.getSensorId()
                                + " value=" + a.getValue())
                );

                List<ScenarioConditionAvro> conditions = e.getConditions().stream()
                        .map(c -> {
                            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                                    .setSensorId(c.getSensorId())
                                    .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                    .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));

                            if (c.getValue() instanceof Integer v) {
                                builder.setValue(v);
                            } else if (c.getValue() instanceof Boolean v) {
                                builder.setValue(v);
                            } else {
                                builder.setValue(null);
                            }

                            return builder.build();
                        })
                        .collect(Collectors.toList());
                List<DeviceActionAvro> actions = e.getActions().stream()
                        .map(a -> DeviceActionAvro.newBuilder()
                                .setSensorId(a.getSensorId())
                                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                .setValue(a.getValue())
                                .build())
                        .collect(Collectors.toList());
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(e.getName())
                        .setConditions(conditions)
                        .setActions(actions)
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(e.getName())
                        .build();
            }
        };

        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();

        send(hubsTopic, event.getHubId(), avro);
    }

    private <T extends SpecificRecordBase> void send(String topic, String key, T avro) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<T> writer = new SpecificDatumWriter<>(avro.getSchema());
            writer.write(avro, encoder);
            encoder.flush();
            producer.send(new ProducerRecord<>(topic, key, out.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException("Ошибка сериализации Avro", e);
        }
    }
}
