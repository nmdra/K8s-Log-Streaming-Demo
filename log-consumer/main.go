package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
)

var log zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339
	log = zerolog.New(os.Stdout).With().
		Str("service", "log-consumer").
		Timestamp().
		Logger()
}

type ESClient struct {
	address  string
	username string
	password string
	client   *http.Client
}

func newESClient(address, username, password, caCert string) (*ESClient, error) {
	tlsCfg := &tls.Config{}

	if caCert != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caCert)) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = pool
	} else {
		return nil, fmt.Errorf("ES_CACERT is required; provide the Elasticsearch CA certificate")
	}

	return &ESClient{
		address:  address,
		username: username,
		password: password,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

func (es *ESClient) indexDocument(index string, doc map[string]interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal document: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc", es.address, index)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(es.username, es.password)

	resp, err := es.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func consumeTopic(ctx context.Context, broker, topic string, esClient *ESClient, wg *sync.WaitGroup) {
	defer wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "log-consumer-" + topic,
		MinBytes: 1,
		MaxBytes: 10e6, // 10 MiB
		MaxWait:  time.Second,
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("error closing reader")
		}
	}()

	log.Info().Str("topic", topic).Str("event", "consumer_started").Msg("")

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info().Str("topic", topic).Str("event", "consumer_stopped").Msg("")
				return
			}
			log.Error().Err(err).Str("topic", topic).Str("event", "read_error").Msg("")
			continue
		}

		var doc map[string]interface{}
		if err := json.Unmarshal(msg.Value, &doc); err != nil {
			log.Warn().
				Err(err).
				Str("topic", topic).
				Str("event", "unmarshal_error").
				Str("raw", string(msg.Value)).
				Msg("")
			continue
		}

		// Enrich document
		now := time.Now().UTC()
		doc["consumed_at"] = now.Format(time.RFC3339)
		doc["source_topic"] = topic
		doc["@timestamp"] = now.Format(time.RFC3339)

		index := "logs-" + now.Format("2006.01.02")

		if err := esClient.indexDocument(index, doc); err != nil {
			log.Error().
				Err(err).
				Str("topic", topic).
				Str("index", index).
				Str("event", "index_error").
				Msg("")
			continue
		}

		log.Info().
			Str("topic", topic).
			Str("index", index).
			Int64("offset", msg.Offset).
			Str("event", "document_indexed").
			Msg("")
	}
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "log-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
	}

	esAddress := os.Getenv("ES_ADDRESS")
	if esAddress == "" {
		esAddress = "https://elasticsearch-es-http.logging.svc:9200"
	}

	esUser := os.Getenv("ES_USERNAME")
	if esUser == "" {
		esUser = "elastic"
	}

	esPassword := os.Getenv("ES_PASSWORD")
	if esPassword == "" {
		log.Fatal().Msg("ES_PASSWORD environment variable is required")
	}

	esCACert := os.Getenv("ES_CACERT")

	esClient, err := newESClient(esAddress, esUser, esPassword, esCACert)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create Elasticsearch client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	topics := []string{"app-logs", "error-logs"}

	for _, topic := range topics {
		wg.Add(1)
		go consumeTopic(ctx, broker, topic, esClient, &wg)
	}

	// Graceful shutdown on SIGTERM / SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	log.Info().Str("event", "shutdown_initiated").Msg("")
	cancel()
	wg.Wait()
	log.Info().Str("event", "shutdown_complete").Msg("")
}
