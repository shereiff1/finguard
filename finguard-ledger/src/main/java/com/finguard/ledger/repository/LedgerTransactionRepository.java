package com.finguard.ledger.repository;

import com.finguard.ledger.domain.LedgerTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LedgerTransactionRepository extends JpaRepository<LedgerTransaction, String> {
}
