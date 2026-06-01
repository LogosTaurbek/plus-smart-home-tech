package ru.yandex.practicum.telemetry.analyzer.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@IdClass(ScenarioActionId.class)
public class ScenarioAction {
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
    @JoinColumn(name = "action_id")
    private Action action;
}
