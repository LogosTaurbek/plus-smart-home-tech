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
import ru.yandex.practicum.telemetry.analyzer.service.HubService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, SpecificRecordBase> hubConsumer;
    private final HubService hubService;

    @Value("${analyzer.kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
        try {
            hubConsumer.subscribe(List.of(hubsTopic));
            log.info("HubEventProcessor subscribed to topic: {}", hubsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records =
                        hubConsumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    try {
                        hubService.handleHubEvent((ru.yandex.practicum.kafka.telemetry.event.HubEventAvro) record.value());
                    } catch (Exception e) {
                        log.error("Error handling hub event", e);
                    }
                }

                if (!records.isEmpty()) {
                    hubConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Error in HubEventProcessor loop", e);
        } finally {
            hubConsumer.close();
        }
    }
}
