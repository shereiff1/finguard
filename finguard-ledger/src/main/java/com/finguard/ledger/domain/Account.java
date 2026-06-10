package com.finguard.ledger.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @Column(name = "id")
    private String id;

    @Column(nullable = false)
    private BigDecimal balance;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private AccountStatus status;

    public enum AccountStatus {
        ACTIVE, FROZEN, UNDER_REVIEW
    }
}
