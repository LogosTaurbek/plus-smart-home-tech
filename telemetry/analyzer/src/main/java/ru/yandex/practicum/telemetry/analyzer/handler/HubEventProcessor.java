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
    @Transactional
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
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
        String hubId = hubEvent.getHubId().toString();
        Object payload = hubEvent.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            String sensorId = deviceAdded.getId().toString();
            Sensor sensor = new Sensor(sensorId, hubId);
            sensorRepository.save(sensor);
            log.info("Saved sensor {} for hub {}", sensorId, hubId);

        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            String sensorId = deviceRemoved.getId().toString();
            sensorRepository.deleteById(sensorId);
            log.info("Removed sensor {} for hub {}", sensorId, hubId);

        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            handleScenarioAdded(hubId, scenarioAdded);

        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            String scenarioName = scenarioRemoved.getName().toString();
            scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                    .ifPresent(scenarioRepository::delete);
            log.info("Removed scenario {} for hub {}", scenarioName, hubId);
        }
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro avro) {
        String name = avro.getName().toString();

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
            String sensorId = c.getSensorId().toString();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresentOrElse(sensor -> {
                Condition condition = new Condition();
                condition.setType(c.getType().name());
                condition.setOperation(c.getOperation().name());

                Object val = c.getValue();
                if (val instanceof Integer intVal) {
                    condition.setValue(intVal);
                } else if (val instanceof Boolean boolVal) {
                    condition.setValue(boolVal ? 1 : 0);
                }

                ScenarioCondition sc = new ScenarioCondition();
                sc.setScenario(scenario);
                sc.setSensor(sensor);
                sc.setCondition(condition);
                scenario.getConditions().add(sc);
            }, () -> log.error("Sensor {} not found for hub {} while adding scenario {}", sensorId, hubId, name));
        }

        for (DeviceActionAvro a : avro.getActions()) {
            String sensorId = a.getSensorId().toString();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresentOrElse(sensor -> {
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
            }, () -> log.error("Sensor {} not found for hub {} while adding action to scenario {}", sensorId, hubId, name));
        }

        scenarioRepository.save(scenario);
        log.info("Saved scenario [{}] for hub {} with {} conditions and {} actions",
                name, hubId, scenario.getConditions().size(), scenario.getActions().size());
    }
}
