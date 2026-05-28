package ru.yandex.practicum.telemetry.collector.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.event.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventServiceImplTest {

    @Mock
    private KafkaEventProducer kafkaEventProducer;

    private EventServiceImpl eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl(kafkaEventProducer);
    }

    @Test
    void shouldHandleLightSensorEvent() {
        LightSensorEvent event = new LightSensorEvent();
        event.setId("s1");
        event.setHubId("h1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setLinkQuality(100);
        event.setLuminosity(50);

        eventService.handleSensorEvent(event);

        ArgumentCaptor<SensorEventAvro> captor = ArgumentCaptor.forClass(SensorEventAvro.class);
        verify(kafkaEventProducer).sendSensorEvent(captor.capture());

        SensorEventAvro avro = captor.getValue();
        assertEquals(event.getId(), avro.getId());
        assertEquals(event.getHubId(), avro.getHubId());
        assertEquals(event.getTimestamp(), avro.getTimestamp());
        assertTrue(avro.getPayload() instanceof LightSensorAvro);
        LightSensorAvro payload = (LightSensorAvro) avro.getPayload();
        assertEquals(event.getLinkQuality(), payload.getLinkQuality());
        assertEquals(event.getLuminosity(), payload.getLuminosity());
    }

    @Test
    void shouldHandleDeviceAddedEvent() {
        DeviceAddedEvent event = new DeviceAddedEvent();
        event.setHubId("h1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setId("d1");
        event.setDeviceType(DeviceType.MOTION_SENSOR);

        eventService.handleHubEvent(event);

        ArgumentCaptor<HubEventAvro> captor = ArgumentCaptor.forClass(HubEventAvro.class);
        verify(kafkaEventProducer).sendHubEvent(captor.capture());

        HubEventAvro avro = captor.getValue();
        assertEquals(event.getHubId(), avro.getHubId());
        assertEquals(event.getTimestamp(), avro.getTimestamp());
        assertTrue(avro.getPayload() instanceof DeviceAddedEventAvro);
        DeviceAddedEventAvro payload = (DeviceAddedEventAvro) avro.getPayload();
        assertEquals(event.getId(), payload.getId());
        assertEquals(DeviceTypeAvro.MOTION_SENSOR, payload.getType());
    }

    @Test
    void shouldHandleScenarioAddedEvent() {
        ScenarioAddedEvent event = new ScenarioAddedEvent();
        event.setHubId("h1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setName("s1");

        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId("c1");
        condition.setType(ConditionType.TEMPERATURE);
        condition.setOperation(ConditionOperation.GREATER_THAN);
        condition.setValue(25);

        DeviceAction action = new DeviceAction();
        action.setSensorId("a1");
        action.setType(ActionType.ACTIVATE);
        action.setValue(1);

        event.setConditions(List.of(condition));
        event.setActions(List.of(action));

        eventService.handleHubEvent(event);

        ArgumentCaptor<HubEventAvro> captor = ArgumentCaptor.forClass(HubEventAvro.class);
        verify(kafkaEventProducer).sendHubEvent(captor.capture());

        HubEventAvro avro = captor.getValue();
        assertTrue(avro.getPayload() instanceof ScenarioAddedEventAvro);
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) avro.getPayload();
        assertEquals(event.getName(), payload.getName());
        assertEquals(1, payload.getConditions().size());
        assertEquals(1, payload.getActions().size());

        ScenarioConditionAvro avroCondition = payload.getConditions().get(0);
        assertEquals(condition.getSensorId(), avroCondition.getSensorId());
        assertEquals(ConditionTypeAvro.TEMPERATURE, avroCondition.getType());
        assertEquals(ConditionOperationAvro.GREATER_THAN, avroCondition.getOperation());
        assertEquals(condition.getValue(), avroCondition.getValue());

        DeviceActionAvro avroAction = payload.getActions().get(0);
        assertEquals(action.getSensorId(), avroAction.getSensorId());
        assertEquals(ActionTypeAvro.ACTIVATE, avroAction.getType());
        assertEquals(action.getValue(), avroAction.getValue());
    }
}
