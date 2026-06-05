package ru.yandex.practicum.telemetry.collector.service.grpc;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubEventHandler {
    HubEventProto.PayloadCase getMessageType();
    void handle(HubEventProto event);
}
