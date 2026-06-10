package com.finguard.ledger.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "ledger_transactions")
public class LedgerTransaction {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "source_account")
    private String sourceAccount;

    @Column(name = "destination_account")
    private String destinationAccount;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Column(name = "fraud_score")
    private BigDecimal fraudScore;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    public enum TransactionStatus {
        PENDING, COMPLETED, ROLLED_BACK
    }
}
