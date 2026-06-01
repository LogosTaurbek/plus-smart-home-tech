package ru.yandex.practicum.telemetry.analyzer.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "sensors")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {
    @Id
    private String id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;
}
