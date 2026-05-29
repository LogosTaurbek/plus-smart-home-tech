package ru.yandex.practicum.telemetry.collector.service;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.practicum.telemetry.collector.model.event.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class EventServiceImplTest {

    @Mock
    private Producer<String, byte[]> producer;

    private EventServiceImpl eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl(producer);
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
}
