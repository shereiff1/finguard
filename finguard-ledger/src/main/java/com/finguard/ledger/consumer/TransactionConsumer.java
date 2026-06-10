package com.finguard.ledger.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finguard.ledger.dto.EvaluatedTransaction;
import com.finguard.ledger.service.LedgerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionConsumer {

    private final LedgerService ledgerService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "transactions.evaluated",
        groupId = "finguard-ledger-group",
        concurrency = "3"
    )
    public void consume(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[KAFKA] Received message from topic={} partition={} offset={}",
                topic, partition, offset);

        try {
            EvaluatedTransaction tx = objectMapper.readValue(message, EvaluatedTransaction.class);
            ledgerService.process(tx);
        } catch (Exception e) {
            log.error("[KAFKA] Failed to process message at offset={} — {}", offset, e.getMessage());
        }
    }
}
