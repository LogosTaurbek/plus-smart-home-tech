package ru.yandex.practicum.telemetry.analyzer.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "scenario_conditions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@IdClass(ScenarioConditionId.class)
public class ScenarioCondition {
    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @Id
    @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "condition_id")
    private Condition condition;
}
