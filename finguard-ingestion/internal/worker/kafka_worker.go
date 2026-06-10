package worker

import (
	"context"
	"finguard-ingestion/internal/domain"
	"log"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type WorkerPool struct {
	writer    *kafka.Writer
	txChannel chan *domain.Transaction
	wg        sync.WaitGroup
}

func NewWorkerPool(writer *kafka.Writer, txChannel chan *domain.Transaction) *WorkerPool {
	return &WorkerPool{
		writer:    writer,
		txChannel: txChannel,
	}
}

func (wp *WorkerPool) Start(ctx context.Context, numWorkers int) {
	log.Printf("Spawning %d background Kafka workers...", numWorkers)

	for i := 0; i < numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

func (wp *WorkerPool) release(tx *domain.Transaction) {
	tx.Reset()
	domain.TxPool.Put(tx)
}

func (wp *WorkerPool) Stop() {
	wp.wg.Wait()
	log.Println("All Kafka workers shut down cleanly.")
}

func (wp *WorkerPool) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()

	for tx := range wp.txChannel {
		if err := tx.Validate(); err != nil {
			log.Printf("[Worker %d] Dropping corrupted payload: %v", workerID, err)
			wp.release(tx)
			continue
		}
		payload, err := json.Marshal(tx)
		if err != nil {
			log.Printf("[Worker %d] Marshalling error: %v", workerID, err)
			wp.release(tx)
			continue
		}

		err = wp.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(tx.AccountID),
			Value: payload,
		})
		if err != nil {
			log.Printf("[Worker %d] Failed to write to Kafka: %v", workerID, err)
		}
		wp.release(tx)
	}
}
