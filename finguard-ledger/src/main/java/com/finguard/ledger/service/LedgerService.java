package com.finguard.ledger.service;

import com.finguard.ledger.domain.Account;
import com.finguard.ledger.domain.LedgerTransaction;
import com.finguard.ledger.dto.EvaluatedTransaction;
import com.finguard.ledger.repository.AccountRepository;
import com.finguard.ledger.repository.LedgerTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerService {

    private final AccountRepository accountRepository;
    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final FraudHandlerService fraudHandlerService;

    @Transactional
    public void process(EvaluatedTransaction dto) {
        log.info("[LEDGER] Processing transaction correlation_id={}", dto.getCorrelationId());

        LedgerTransaction tx = new LedgerTransaction();
        tx.setId(dto.getCorrelationId());
        tx.setSourceAccount(dto.getAccountId());
        tx.setDestinationAccount(dto.getDestinationAccountId());
        tx.setAmount(dto.getAmount());
        tx.setCurrency(dto.getCurrency());
        tx.setStatus(LedgerTransaction.TransactionStatus.PENDING);
        tx.setCreatedAt(LocalDateTime.now());
        ledgerTransactionRepository.save(tx);

        if (fraudHandlerService.isFraudulent(dto.getFraudScore())) {
            fraudHandlerService.handle(tx, dto.getFraudScore());
            return;
        }

        Account sender = accountRepository.findByIdWithLock(dto.getAccountId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Sender account not found: " + dto.getAccountId()));

        if (sender.getBalance().compareTo(dto.getAmount()) < 0) {
            log.error("[LEDGER] Insufficient funds for account {}", sender.getId());
            tx.setStatus(LedgerTransaction.TransactionStatus.ROLLED_BACK);
            ledgerTransactionRepository.save(tx);
            return;
        }

        sender.setBalance(sender.getBalance().subtract(dto.getAmount()));
        accountRepository.save(sender);

        Account receiver = accountRepository.findByIdWithLock(dto.getDestinationAccountId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Receiver account not found: " + dto.getDestinationAccountId()));

        receiver.setBalance(receiver.getBalance().add(dto.getAmount()));
        accountRepository.save(receiver);

        tx.setStatus(LedgerTransaction.TransactionStatus.COMPLETED);
        tx.setFraudScore(dto.getFraudScore());
        ledgerTransactionRepository.save(tx);

        log.info("[LEDGER] Transaction {} COMPLETED — {} {} from {} to {}",
                tx.getId(), dto.getAmount(), dto.getCurrency(),
                dto.getAccountId(), dto.getDestinationAccountId());
    }
}
