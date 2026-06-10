package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"finguard-ingestion/internal/domain"
	"finguard-ingestion/internal/transport"
	"finguard-ingestion/internal/worker"

	"github.com/segmentio/kafka-go"
)

func main() {
	log.Println("Starting FinGuard Ingestion Engine...")

	txChannel := make(chan *domain.Transaction, 10000)

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP("kafka:29092"),
		Topic:        "transactions.raw",
		Async:        true,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer kafkaWriter.Close()

	workerPool := worker.NewWorkerPool(kafkaWriter, txChannel)
	workerPool.Start(context.Background(), 50)

	wsHandler := transport.NewWebSocketHandler(txChannel)
	http.HandleFunc("/api/v1/stream", wsHandler.HandleStream)

	server := &http.Server{Addr: ":8080"}

	go func() {
		log.Println("Gateway listening on http://localhost:8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Network failure: %v", err)
		}
	}()

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
	<-stopSignal

	log.Println("Shutting down gateway gracefully...")

	_ = server.Shutdown(context.Background())

	close(txChannel)

	workerPool.Stop()
}
