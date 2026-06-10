package transport

import (
	"log"
	"net/http"
	"time"

	"finguard-ingestion/internal/domain"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type WebSocketHandler struct {
	TxChannel chan *domain.Transaction
}

func NewWebSocketHandler(txChannel chan *domain.Transaction) *WebSocketHandler {
	return &WebSocketHandler{TxChannel: txChannel}
}

func (h *WebSocketHandler) HandleStream(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade handshake failed: %v", err)
		return
	}
	defer conn.Close()
	log.Println("New live telemetry pipeline connected successfully!")

	for {
		tx := domain.TxPool.Get().(*domain.Transaction)

		_, messageBytes, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Client disconnected or pipeline broken: %v", err)
			tx.Reset()
			domain.TxPool.Put(tx)
			break
		}

		if err := json.Unmarshal(messageBytes, tx); err != nil {
			log.Printf("Malformed streaming payload skipped: %v", err)
			tx.Reset()
			domain.TxPool.Put(tx)
			continue
		}

		tx.CorrelationID = uuid.NewString()
		tx.Timestamp = time.Now().UTC()

		select {
		case h.TxChannel <- tx:
		default:
			tx.Reset()
			domain.TxPool.Put(tx)
			log.Println("Backpressure alert: dropping incoming transaction stream payload")
		}
	}
}
