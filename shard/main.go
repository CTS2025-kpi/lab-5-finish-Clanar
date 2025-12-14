package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Record struct {
	Value     string
	Timestamp int64
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Record
}

type ReplicationEvent struct {
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

var (
	store          = Store{data: make(map[string]Record)}
	role           = os.Getenv("ROLE")
	shardName      = os.Getenv("SHARD_NAME")
	replicaID      = os.Getenv("REPLICA_ID")
	rabbitURL      = os.Getenv("RABBIT_URL")
	coordinatorURL = os.Getenv("COORDINATOR_URL")
	buildVersion   = "1.0.0"
)

var (
	shardRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shard_http_requests_total",
			Help: "Total number of HTTP requests on shard",
		},
		[]string{"method", "replica_id"},
	)
	replicationLag = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shard_replication_lag_seconds",
			Help: "Difference between now and write timestamp",
		},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "shard_http_request_duration_seconds",
			Help:    "Duration of HTTP requests on shard",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
	cpuUsage = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shard_cpu_usage",
			Help: "Current CPU usage (0-1)",
		},
	)
	memoryUsage = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shard_memory_usage_bytes",
			Help: "Current memory usage in bytes",
		},
	)
	dataSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shard_data_size_bytes",
			Help: "Total size of stored data in bytes",
		},
	)
)

func init() {
	prometheus.MustRegister(shardRequestsTotal)
	prometheus.MustRegister(replicationLag)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(cpuUsage)
	prometheus.MustRegister(memoryUsage)
	prometheus.MustRegister(dataSize)
}

func main() {
	if coordinatorURL == "" {
		coordinatorURL = "http://coordinator:8080"
	}

	var conn *amqp.Connection
	var err error
	for i := 0; i < 15; i++ {
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			break
		}
		log.Println("Waiting for RabbitMQ...", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	exchangeName := fmt.Sprintf("%s-events", shardName)
	err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	if role == "follower" {
		qName := fmt.Sprintf("q-%s-%s", shardName, replicaID)
		q, err := ch.QueueDeclare(qName, true, false, false, false, nil)
		if err != nil {
			log.Fatal(err)
		}

		err = ch.QueueBind(q.Name, "", exchangeName, false, nil)
		if err != nil {
			log.Fatal(err)
		}

		msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			for d := range msgs {
				var event ReplicationEvent
				json.Unmarshal(d.Body, &event)

				store.mu.Lock()
				current, exists := store.data[event.Key]

				if event.Op == "PUT" {
					if !exists || event.Timestamp > current.Timestamp {
						store.data[event.Key] = Record{Value: event.Value, Timestamp: event.Timestamp}

						lag := time.Since(time.Unix(0, event.Timestamp)).Seconds()
						replicationLag.Set(lag)

						log.Printf("[REPLICA %s] LWW Update Key=%s (Lag: %.4fs)", replicaID, event.Key, lag)
					} else {
						log.Printf("[REPLICA %s] Ignored stale update for Key=%s", replicaID, event.Key)
					}
				} else if event.Op == "DELETE" {
					delete(store.data, event.Key)
				}

				updateDataSizeMetric()
				store.mu.Unlock()
				d.Ack(false)
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			collectSystemMetrics()
			reportMetricsToCoordinator()
		}
	}()

	http.HandleFunc("/data", metricsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Trace-ID")
		log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Received data request", "method":"%s", "key":"%s"}`, traceID, r.Method, r.URL.Query().Get("key"))
		shardRequestsTotal.WithLabelValues(r.Method, replicaID).Inc()

		key := r.URL.Query().Get("key")

		if r.Method == "GET" {
			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Handling GET request"}`, traceID)
			store.mu.RLock()
			rec, ok := store.data[key]
			store.mu.RUnlock()

			if !ok {
				log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Key not found"}`, traceID)
				http.Error(w, "Not found", 404)
				return
			}

			w.Header().Set("X-Replica-ID", replicaID)
			w.Header().Set("X-Trace-ID", traceID)
			fmt.Fprintf(w, "%s", rec.Value)
			log.Printf(`{"level":"info", "trace_id":"%s", "msg":"Read success", "key":"%s", "replica":"%s"}`, traceID, key, replicaID)
			return
		}

		if role == "leader" && (r.Method == "POST" || r.Method == "PUT") {
			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Handling write request"}`, traceID)
			body, _ := io.ReadAll(r.Body)
			val := string(body)
			now := time.Now().UnixNano()

			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Acquiring store lock"}`, traceID)
			store.mu.Lock()
			store.data[key] = Record{Value: val, Timestamp: now}
			updateDataSizeMetric()
			store.mu.Unlock()
			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Store update complete"}`, traceID)

			event := ReplicationEvent{Op: "PUT", Key: key, Value: val, Timestamp: now}
			jsonBody, _ := json.Marshal(event)
			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Publishing to RabbitMQ"}`, traceID)

			err = ch.Publish(
				exchangeName, "", false, false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        jsonBody,
				},
			)
			if err != nil {
				log.Printf(`{"level":"error", "trace_id":"%s", "msg":"RabbitMQ publish failed", "error":"%v"}`, traceID, err)
				http.Error(w, "Replication failed", 500)
				return
			}
			log.Printf(`{"level":"info", "trace_id":"%s", "msg":"Written & Published", "key":"%s"}`, traceID, key)
			w.WriteHeader(http.StatusOK)
		} else {
			log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Method not allowed or not leader"}`, traceID)
			http.Error(w, "Method not allowed or not a leader", 405)
		}
	}))

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy", "role": role, "replica": replicaID})
	})

	http.HandleFunc("/debug/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "echo test", "shard": shardName, "replica": replicaID})
	})

	http.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": buildVersion, "shard": shardName})
	})

	http.HandleFunc("/metrics/internal", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		store.mu.RLock()
		dataCount := len(store.data)
		store.mu.RUnlock()

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)

		metrics := map[string]interface{}{
			"data_count":      dataCount,
			"cpu_usage":       cpuUsage,
			"memory_usage":    memoryUsage,
			"data_size":       dataSize,
			"replication_lag": replicationLag,
		}

		json.NewEncoder(w).Encode(metrics)
	})

	http.Handle("/metrics", promhttp.Handler())

	log.Printf("Starting %s (%s) on :8080", role, replicaID)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			duration := time.Since(start).Seconds()
			httpRequestDuration.WithLabelValues(r.Method).Observe(duration)
		}()
		next(w, r)
	}
}

func collectSystemMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	cpuUsage.Set(0.3 + (rand.Float64() * 0.4))
	memoryUsage.Set(float64(memStats.Alloc))
}

func updateDataSizeMetric() {
	var totalSize int64
	for _, record := range store.data {
		totalSize += int64(len(record.Value))
	}
	dataSize.Set(float64(totalSize))
}

func reportMetricsToCoordinator() {
	if coordinatorURL == "" {
		return
	}

	store.mu.RLock()
	dataCount := len(store.data)
	store.mu.RUnlock()

	baseLatency := 0.05 + (float64(dataCount) / 1000.0 * 0.1)
	if baseLatency > 0.5 {
		baseLatency = 0.5
	}

	simulatedP99 := baseLatency * (1.0 + (rand.Float64()*0.3 - 0.15))

	metrics := map[string]interface{}{
		"shard_name":  shardName,
		"latency_p99": simulatedP99,
		"cpu_usage":   cpuUsage,
	}

	jsonBody, _ := json.Marshal(metrics)

	resp, err := http.Post(coordinatorURL+"/metrics/report", "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Failed to report metrics to coordinator: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Coordinator returned non-OK status: %d", resp.StatusCode)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
