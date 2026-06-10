package domain

import (
	"testing"
)

func BenchmarkStandardAllocation(b *testing.B) {

	for i := 0; i < b.N; i++ {
		tx := &Transaction{
			AccountID:            "ACC-123",
			DestinationAccountID: "ACC-456",
			Amount:               150.50,
			Currency:             "USD",
		}
		if err := tx.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkPoolAllocation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tx := TxPool.Get().(*Transaction)

		tx.AccountID = "ACC-123"
		tx.DestinationAccountID = "ACC-456"
		tx.Amount = 150.50
		tx.Currency = "USD"
		if err := tx.Validate(); err != nil {
			b.Fatal(err)
		}
		tx.Reset()
		TxPool.Put(tx)
	}
}
