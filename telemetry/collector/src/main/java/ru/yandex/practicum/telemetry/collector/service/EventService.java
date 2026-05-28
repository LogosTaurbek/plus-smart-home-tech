package ru.yandex.practicum.telemetry.collector.service;

import ru.yandex.practicum.telemetry.collector.model.event.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.event.SensorEvent;

public interface EventService {
    void handleSensorEvent(SensorEvent event);
    void handleHubEvent(HubEvent event);
}
