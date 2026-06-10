package com.finguard.ledger.service;

import com.finguard.ledger.domain.Account;
import com.finguard.ledger.domain.FraudAlert;
import com.finguard.ledger.domain.LedgerTransaction;
import com.finguard.ledger.repository.AccountRepository;
import com.finguard.ledger.repository.FraudAlertRepository;
import com.finguard.ledger.repository.LedgerTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudHandlerService {

    private final AccountRepository accountRepository;
    private final FraudAlertRepository fraudAlertRepository;
    private final LedgerTransactionRepository ledgerTransactionRepository;

    private static final BigDecimal FRAUD_THRESHOLD = new BigDecimal("0.85");

    @Transactional
    public void handle(LedgerTransaction tx, BigDecimal fraudScore) {

        log.warn("[FRAUD] High fraud score {} detected for correlation_id={}",
                fraudScore, tx.getId());

        FraudAlert alert = new FraudAlert();
        alert.setCorrelationId(tx.getId());
        alert.setAccountId(tx.getSourceAccount());
        alert.setFraudScore(fraudScore);
        alert.setCreatedAt(LocalDateTime.now());
        fraudAlertRepository.save(alert);

        accountRepository.findByIdWithLock(tx.getSourceAccount())
                .ifPresent(account -> {
                    account.setStatus(Account.AccountStatus.FROZEN);
                    accountRepository.save(account);
                    log.warn("[FRAUD] Account {} has been FROZEN", account.getId());
                });

        tx.setStatus(LedgerTransaction.TransactionStatus.ROLLED_BACK);
        tx.setFraudScore(fraudScore);
        ledgerTransactionRepository.save(tx);

        log.warn("[FRAUD] Transaction {} ROLLED_BACK due to fraud score {}",
                tx.getId(), fraudScore);
    }

    public boolean isFraudulent(BigDecimal fraudScore) {
        return fraudScore != null && fraudScore.compareTo(FRAUD_THRESHOLD) > 0;
    }
}
