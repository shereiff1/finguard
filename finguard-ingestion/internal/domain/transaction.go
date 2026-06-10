package domain

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidAccount     = errors.New("source or destination account ID cannot be empty")
	ErrInvalidTransaction = errors.New("transaction amount must be positive")
	ErrInvalidCurrency    = errors.New("currency must be a valid 3-letter ISO code")
)

type Transaction struct {
	CorrelationID        string    `json:"correlation_id"`
	AccountID            string    `json:"account_id"`
	DestinationAccountID string    `json:"destination_account_id"`
	Amount               float64   `json:"amount"`
	Currency             string    `json:"currency"`
	Timestamp            time.Time `json:"timestamp"`
	IPAddress            string    `json:"ip_address"`
	DeviceID             string    `json:"device_id"`
}

func (t *Transaction) Reset() {
	t.CorrelationID = ""
	t.AccountID = ""
	t.DestinationAccountID = ""
	t.Amount = 0.0
	t.Currency = ""
	t.Timestamp = time.Time{}
	t.IPAddress = ""
	t.DeviceID = ""
}

func (t *Transaction) Validate() error {
	if t.AccountID == "" || t.DestinationAccountID == "" {
		return ErrInvalidAccount
	}
	if t.Amount <= 0 {
		return ErrInvalidTransaction
	}
	if len(t.Currency) != 3 {
		return ErrInvalidCurrency
	}
	return nil
}

var TxPool = sync.Pool{
	New: func() interface{} {
		return &Transaction{}
	},
}
