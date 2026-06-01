package ru.yandex.practicum.telemetry.analyzer.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ScenarioConditionId implements Serializable {
    private Long scenario;
    private String sensor;
    private Long condition;
}
