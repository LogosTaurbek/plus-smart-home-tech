package ru.yandex.practicum.telemetry.collector.service.grpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import java.time.Instant;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {
    private final KafkaEventProducer kafkaEventProducer;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        ScenarioAddedEventProto proto = event.getScenarioAdded();
        log.info("Handling SCENARIO_ADDED for hub: {}, scenario: {}", event.getHubId(), proto.getId());

        ScenarioAddedEventAvro avro = ScenarioAddedEventAvro.newBuilder()
                .setName(proto.getId())
                .setConditions(proto.getConditionsList().stream()
                        .map(c -> {
                            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                                    .setSensorId(c.getSensorId())
                                    .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                    .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));

                            if (c.getValueCase() == ScenarioConditionProto.ValueCase.INT_VALUE) {
                                builder.setValue(c.getIntValue());
                            } else if (c.getValueCase() == ScenarioConditionProto.ValueCase.BOOL_VALUE) {
                                builder.setValue(c.getBoolValue());
                            }
                            return builder.build();
                        })
                        .collect(Collectors.toList()))
                .setActions(proto.getActionsList().stream()
                        .map(a -> DeviceActionAvro.newBuilder()
                                .setSensorId(a.getSensorId())
                                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                .setValue(a.getValue())
                                .build())
                        .collect(Collectors.toList()))
                .build();

        HubEventAvro hubEventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(avro)
                .build();

        kafkaEventProducer.sendHubEvent(hubEventAvro);
    }
}
