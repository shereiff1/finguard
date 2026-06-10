CREATE TABLE accounts (
    id VARCHAR(50) PRIMARY KEY,
    balance DECIMAL(18, 4) NOT NULL CHECK (balance >= 0.0),
    status VARCHAR(20) NOT NULL
);
CREATE TABLE ledger_transactions (
    id VARCHAR(50) PRIMARY KEY,
    source_account VARCHAR(50) REFERENCES accounts(id),
    destination_account VARCHAR(50) REFERENCES accounts(id),
    amount DECIMAL(18, 4) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    fraud_score DECIMAL(5, 4),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE fraud_alerts (
    id BIGSERIAL PRIMARY KEY,
    correlation_id VARCHAR(50) REFERENCES ledger_transactions(id),
    account_id VARCHAR(50) REFERENCES accounts(id),
    fraud_score DECIMAL(5, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO accounts (id, balance, status) VALUES
    ('ACC-99210', 100000.00, 'ACTIVE'),
    ('ACC-11029', 100000.00, 'ACTIVE'),
    ('ACC-00123', 100000.00, 'ACTIVE'),
    ('ACC-00456', 100000.00, 'ACTIVE'),
    ('ACC-77777', 500000.00, 'ACTIVE'),
    ('ACC-88888', 500000.00, 'ACTIVE');
