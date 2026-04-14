package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
)

var log zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339
	rand.Seed(time.Now().UnixNano())
	log = zerolog.New(os.Stdout).With().
		Str("service", "order-service").
		Timestamp().
		Logger()
}

type Order struct {
	ID       string  `json:"id"`
	Item     string  `json:"item"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
	Status   string  `json:"status"`
}

var items = []string{"laptop", "phone", "headphones", "tablet", "monitor"}
var statuses = []string{"pending", "processing", "shipped", "delivered"}

func randomOrder() Order {
	return Order{
		ID:       uuid.New().String(),
		Item:     items[rand.Intn(len(items))],
		Quantity: rand.Intn(5) + 1,
		Price:    float64(rand.Intn(90000)+1000) / 100,
		Status:   statuses[rand.Intn(len(statuses))],
	}
}

func withRequestLog(next func(w http.ResponseWriter, r *http.Request, requestID string, start time.Time)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.New().String()
		start := time.Now()
		next(w, r, requestID, start)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func listOrdersHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	start := time.Now()

	orders := make([]Order, rand.Intn(5)+1)
	for i := range orders {
		orders[i] = randomOrder()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orders)

	log.Info().
		Str("request_id", requestID).
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Int("status", http.StatusOK).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("event", "request_completed").
		Msg("")
}

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	start := time.Now()
	id := mux.Vars(r)["id"]

	// 10% chance → 404 error
	if rand.Float64() < 0.10 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "order not found"})

		log.Error().
			Str("request_id", requestID).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("order_id", id).
			Int("status", http.StatusNotFound).
			Int64("duration_ms", time.Since(start).Milliseconds()).
			Str("event", "order_not_found").
			Msg("")
		return
	}

	// 15% chance → slow response
	if rand.Float64() < 0.15 {
		delay := time.Duration(rand.Intn(1500)+500) * time.Millisecond
		time.Sleep(delay)

		log.Warn().
			Str("request_id", requestID).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("order_id", id).
			Int64("duration_ms", delay.Milliseconds()).
			Str("event", "slow_request").
			Msg("")
	}

	order := randomOrder()
	order.ID = id

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(order)

	log.Info().
		Str("request_id", requestID).
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Str("order_id", id).
		Int("status", http.StatusOK).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("event", "request_completed").
		Msg("")
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	start := time.Now()

	order := randomOrder()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(order)

	log.Info().
		Str("request_id", requestID).
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Str("order_id", order.ID).
		Str("item", order.Item).
		Int("quantity", order.Quantity).
		Float64("price", order.Price).
		Int("status", http.StatusCreated).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("event", "order_created").
		Msg("")
}

func getPaymentHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	start := time.Now()
	id := mux.Vars(r)["id"]

	// 20% chance → payment failure
	if rand.Float64() < 0.20 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "payment failed"})

		log.Error().
			Str("request_id", requestID).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("payment_id", id).
			Int("status", http.StatusPaymentRequired).
			Int64("duration_ms", time.Since(start).Milliseconds()).
			Str("event", "payment_failed").
			Msg("")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"payment_id": id,
		"status":     "success",
		"amount":     float64(rand.Intn(50000)+500) / 100,
	})

	log.Info().
		Str("request_id", requestID).
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Str("payment_id", id).
		Int("status", http.StatusOK).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("event", "payment_completed").
		Msg("")
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Validate port is a number
	if _, err := strconv.Atoi(port); err != nil {
		log.Fatal().Str("port", port).Msg("invalid PORT value")
	}

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods(http.MethodGet)
	r.HandleFunc("/orders", listOrdersHandler).Methods(http.MethodGet)
	r.HandleFunc("/orders/{id}", getOrderHandler).Methods(http.MethodGet)
	r.HandleFunc("/orders", createOrderHandler).Methods(http.MethodPost)
	r.HandleFunc("/payments/{id}", getPaymentHandler).Methods(http.MethodGet)

	log.Info().Str("port", port).Str("event", "server_starting").Msg("")

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("server error")
	}
}
