package com.finguard.ledger.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class EvaluatedTransaction {

    @JsonProperty("correlation_id")
    private String correlationId;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("destination_account_id")
    private String destinationAccountId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("timestamp")
    private LocalDateTime timestamp;

    @JsonProperty("fraud_score")
    private BigDecimal fraudScore;

    @JsonProperty("ip_address")
    private String ipAddress;

    @JsonProperty("device_id")
    private String deviceId;
}
