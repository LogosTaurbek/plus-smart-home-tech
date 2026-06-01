package ru.yandex.practicum.telemetry.analyzer.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, SpecificRecordBase> hubConsumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Value("${analyzer.kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void run() {
        try {
            hubConsumer.subscribe(List.of(hubsTopic));
            log.info("HubEventProcessor subscribed to topic: {}", hubsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records =
                        hubConsumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    handleHubEvent(record.value());
                }

                if (!records.isEmpty()) {
                    hubConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            hubConsumer.close();
        }
    }

    private void handleHubEvent(SpecificRecordBase event) {
        if (!(event instanceof HubEventAvro hubEvent)) return;
        String hubId = hubEvent.getHubId();
        Object payload = hubEvent.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            Sensor sensor = new Sensor(deviceAdded.getId(), hubId);
            sensorRepository.save(sensor);
            log.debug("Saved sensor {} for hub {}", deviceAdded.getId(), hubId);

        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            sensorRepository.deleteById(deviceRemoved.getId());
            log.debug("Removed sensor {} for hub {}", deviceRemoved.getId(), hubId);

        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            handleScenarioAdded(hubId, scenarioAdded);

        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            scenarioRepository.findByHubIdAndName(hubId, scenarioRemoved.getName())
                    .ifPresent(scenarioRepository::delete);
            log.debug("Removed scenario {} for hub {}", scenarioRemoved.getName(), hubId);
        }
    }

    @Transactional
    protected void handleScenarioAdded(String hubId, ScenarioAddedEventAvro avro) {
        String name = avro.getName();

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, name)
                .orElseGet(() -> {
                    Scenario s = new Scenario();
                    s.setHubId(hubId);
                    s.setName(name);
                    return s;
                });

        scenario.getConditions().clear();
        scenario.getActions().clear();

        for (ScenarioConditionAvro c : avro.getConditions()) {
            String sensorId = c.getSensorId();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
                Condition condition = new Condition();
                condition.setType(c.getType().name());
                condition.setOperation(c.getOperation().name());

                Object val = c.getValue();
                if (val instanceof Integer intVal) {
                    condition.setValue(intVal);
                } else if (val instanceof Boolean boolVal) {
                    condition.setValue(boolVal ? 1 : 0);
                }
                // null means Hub Router didn't send a value (e.g. MOTION) — threshold stays null

                ScenarioCondition sc = new ScenarioCondition();
                sc.setScenario(scenario);
                sc.setSensor(sensor);
                sc.setCondition(condition);
                scenario.getConditions().add(sc);
            });
        }

        for (DeviceActionAvro a : avro.getActions()) {
            String sensorId = a.getSensorId();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
                Action action = new Action();
                action.setType(a.getType().name());
                if (a.getValue() instanceof Integer intVal) {
                    action.setValue(intVal);
                }

                ScenarioAction sa = new ScenarioAction();
                sa.setScenario(scenario);
                sa.setSensor(sensor);
                sa.setAction(action);
                scenario.getActions().add(sa);
            });
        }

        scenarioRepository.save(scenario);
        log.info("Saved scenario [{}] for hub {} with {} conditions and {} actions",
                name, hubId, scenario.getConditions().size(), scenario.getActions().size());
    }
}
