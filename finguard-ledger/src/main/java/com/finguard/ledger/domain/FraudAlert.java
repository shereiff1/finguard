package com.finguard.ledger.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "fraud_alerts")
public class FraudAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "account_id")
    private String accountId;

    @Column(name = "fraud_score", nullable = false)
    private BigDecimal fraudScore;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}
