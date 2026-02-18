package main

import (
	"context"
	"encoding/json"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

func main() {

	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	log.Println("Kafka brokers:", kafkaBrokers)

	go startConsumer("movie-events")
	go startConsumer("user-events")
	go startConsumer("payment-events")

	http.HandleFunc("/api/events/movie", createMovieEvent)
	http.HandleFunc("/api/events/user", createUserEvent)
	http.HandleFunc("/api/events/payment", createPaymentEvent)
	http.HandleFunc("/api/events/health", health)

	log.Println("Events service started on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":true}`))
}

func createMovieEvent(w http.ResponseWriter, r *http.Request) {
	createEvent(w, r, "movie-events", "movie")
}

func createUserEvent(w http.ResponseWriter, r *http.Request) {
	createEvent(w, r, "user-events", "user")
}

func createPaymentEvent(w http.ResponseWriter, r *http.Request) {
	createEvent(w, r, "payment-events", "payment")
}

func createEvent(w http.ResponseWriter, r *http.Request, topic string, eventType string) {

	var payload interface{}
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano()),
		Type:      eventType,
		Timestamp: time.Now(),
		Payload:   payload,
	}

	eventBytes, _ := json.Marshal(event)

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokers},
		Topic:   topic,
	})

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.ID),
			Value: eventBytes,
		},
	)

	if err != nil {
		log.Println("Kafka write error:", err)
		http.Error(w, "Kafka error", http.StatusInternalServerError)
		return
	}

	log.Println("Produced event:", event.ID)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"id":     event.ID,
	})
}

func startConsumer(topic string) {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    topic,
		GroupID:  "cinemaabyss-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Println("Consumer started for topic:", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Consumer error:", err)
			continue
		}

		log.Printf("Consumed from %s → key=%s value=%s\n",
			topic,
			string(msg.Key),
			string(msg.Value),
		)
	}
}
