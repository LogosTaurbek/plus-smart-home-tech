package ru.yandex.practicum.telemetry.collector.service;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.collector.model.event.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventServiceImplTest {

    @Mock
    private Producer<String, byte[]> producer;

    private EventServiceImpl eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl(producer);
        ReflectionTestUtils.setField(eventService, "sensorsTopic", "telemetry.sensors.v1");
        ReflectionTestUtils.setField(eventService, "hubsTopic", "telemetry.hubs.v1");
    }

    @Test
    void shouldHandleLightSensorEvent() {
        LightSensorEvent event = new LightSensorEvent();
        event.setId("s1");
        event.setHubId("h1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setLinkQuality(100);
        event.setLuminosity(50);

        assertDoesNotThrow(() -> eventService.handleSensorEvent(event));
    }

    @Test
    void shouldHandleDeviceAddedEvent() {
        DeviceAddedEvent event = new DeviceAddedEvent();
        event.setHubId("h1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setId("d1");
        event.setDeviceType(DeviceType.MOTION_SENSOR);

        assertDoesNotThrow(() -> eventService.handleHubEvent(event));
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

        assertDoesNotThrow(() -> eventService.handleHubEvent(event));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSerializeScenarioConditionValueCorrectly() throws Exception {
        ScenarioAddedEvent event = new ScenarioAddedEvent();
        event.setHubId("hub-test");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setName("test-scenario");

        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId("sensor-1");
        condition.setType(ConditionType.CO2LEVEL);
        condition.setOperation(ConditionOperation.GREATER_THAN);
        condition.setValue(500);

        DeviceAction action = new DeviceAction();
        action.setSensorId("act-1");
        action.setType(ActionType.ACTIVATE);

        event.setConditions(List.of(condition));
        event.setActions(List.of(action));

        eventService.handleHubEvent(event);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        byte[] bytes = captor.getValue().value();

        SpecificDatumReader<HubEventAvro> reader = new SpecificDatumReader<>(HubEventAvro.getClassSchema());
        HubEventAvro hubEvent = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        ScenarioAddedEventAvro scenario = (ScenarioAddedEventAvro) hubEvent.getPayload();
        ScenarioConditionAvro cond = scenario.getConditions().get(0);

        Object value = cond.getValue();
        System.out.println("Condition value type: " + (value == null ? "null" : value.getClass().getName()));
        System.out.println("Condition value: " + value);

        assertEquals(Integer.class, value.getClass(), "Value should be Integer");
        assertEquals(500, ((Integer) value).intValue(), "Value should be 500");
    }
}
