package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coordinator_http_requests_total",
			Help: "Total number of HTTP requests on coordinator",
		},
		[]string{"method", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "coordinator_http_request_duration_seconds",
			Help:    "Duration of HTTP requests on coordinator",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
	autoscalingEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coordinator_autoscaling_events_total",
			Help: "Total number of autoscaling events",
		},
		[]string{"action", "reason"},
	)
	shardCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "coordinator_shard_count",
			Help: "Current number of shards",
		},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(autoscalingEvents)
	prometheus.MustRegister(shardCount)
}

type ConsistentHashRing struct {
	mu          sync.RWMutex
	nodes       []string
	replicas    int
	hashToNode  map[uint32]string
	nodeToHash  map[string][]uint32
	nodeWeights map[string]int
}

func NewConsistentHashRing(replicas int) *ConsistentHashRing {
	return &ConsistentHashRing{
		replicas:    replicas,
		hashToNode:  make(map[uint32]string),
		nodeToHash:  make(map[string][]uint32),
		nodeWeights: make(map[string]int),
	}
}

func (r *ConsistentHashRing) hash(key string) uint32 {
	var hash uint32 = 5381
	for _, c := range key {
		hash = ((hash << 5) + hash) + uint32(c)
	}
	return hash
}

func (r *ConsistentHashRing) AddNode(node string, weight int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if weight <= 0 {
		weight = 1
	}
	r.nodeWeights[node] = weight

	totalVirtualNodes := r.replicas * weight
	for i := 0; i < totalVirtualNodes; i++ {
		virtualNode := fmt.Sprintf("%s-%d", node, i)
		hash := r.hash(virtualNode)
		r.hashToNode[hash] = node
		r.nodeToHash[node] = append(r.nodeToHash[node], hash)
	}

	r.sortRing()
	shardCount.Set(float64(len(r.nodes)))
}

func (r *ConsistentHashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeWeights[node]; !exists {
		return
	}

	for _, hash := range r.nodeToHash[node] {
		delete(r.hashToNode, hash)
	}
	delete(r.nodeToHash, node)
	delete(r.nodeWeights, node)

	r.sortRing()
	shardCount.Set(float64(len(r.nodes)))
}

func (r *ConsistentHashRing) sortRing() {
	hashes := make([]uint32, 0, len(r.hashToNode))
	for hash := range r.hashToNode {
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })

	r.nodes = make([]string, 0, len(hashes))
	for _, hash := range hashes {
		r.nodes = append(r.nodes, r.hashToNode[hash])
	}
}

func (r *ConsistentHashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return ""
	}

	hash := r.hash(key)

	var targetNode string
	for h, node := range r.hashToNode {
		if h >= hash {
			targetNode = node
			break
		}
	}

	if targetNode == "" && len(r.nodes) > 0 {
		for _, node := range r.nodes {
			targetNode = node
			break
		}
	}

	return targetNode
}

func (r *ConsistentHashRing) GetAllNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodeWeights))
	for node := range r.nodeWeights {
		nodes = append(nodes, node)
	}
	return nodes
}

type ShardInfo struct {
	Name      string    `json:"name"`
	LeaderURL string    `json:"leader_url"`
	Replicas  []string  `json:"replicas"`
	Status    string    `json:"status"`
	Load      float64   `json:"load"`
	LastCheck time.Time `json:"last_check"`
}

type Autoscaler struct {
	mu                sync.Mutex
	ring              *ConsistentHashRing
	shards            map[string]*ShardInfo
	metrics           map[string][]float64
	latencyThreshold  float64
	cpuThreshold      float64
	cooldownPeriod    time.Duration
	lastScaleUpTime   time.Time
	lastScaleDownTime time.Time
	minShards         int
	maxShards         int
}

func NewAutoscaler(ring *ConsistentHashRing, minShards, maxShards int) *Autoscaler {
	return &Autoscaler{
		ring:             ring,
		shards:           make(map[string]*ShardInfo),
		metrics:          make(map[string][]float64),
		latencyThreshold: 0.05,
		cpuThreshold:     0.1,
		cooldownPeriod:   5 * time.Minute,
		minShards:        minShards,
		maxShards:        maxShards,
	}
}

func (a *Autoscaler) AddShard(name, leaderURL string, replicas []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.shards[name] = &ShardInfo{
		Name:      name,
		LeaderURL: leaderURL,
		Replicas:  replicas,
		Status:    "healthy",
		Load:      0.0,
		LastCheck: time.Now(),
	}

	a.ring.AddNode(leaderURL, 1)
}

func (a *Autoscaler) RemoveShard(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if shard, exists := a.shards[name]; exists {
		a.ring.RemoveNode(shard.LeaderURL)
		delete(a.shards, name)
		delete(a.metrics, name)
	}
}

func (a *Autoscaler) ReportMetrics(shardName string, latencyP99, cpuUsage float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.shards[shardName]; !exists {
		return
	}

	if len(a.metrics[shardName]) >= 10 {
		a.metrics[shardName] = a.metrics[shardName][1:]
	}
	a.metrics[shardName] = append(a.metrics[shardName], latencyP99)

	shard := a.shards[shardName]
	shard.Load = latencyP99
	shard.LastCheck = time.Now()
}

func (a *Autoscaler) CheckScaling() (shouldScaleUp, shouldScaleDown bool, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentShards := len(a.shards)
	if currentShards >= a.maxShards {
		return false, false, "max shards reached"
	}
	if currentShards <= a.minShards {
		return false, false, "min shards limit"
	}

	now := time.Now()
	if now.Sub(a.lastScaleUpTime) < a.cooldownPeriod {
		return false, false, "scale-up cooldown"
	}
	if now.Sub(a.lastScaleDownTime) < a.cooldownPeriod {
		return false, false, "scale-down cooldown"
	}

	var totalLatency, totalCPU float64
	shardCount := 0

	for name, shard := range a.shards {
		if len(a.metrics[name]) > 0 {
			totalLatency += a.metrics[name][len(a.metrics[name])-1]
			totalCPU += shard.Load
			shardCount++
		}
	}

	if shardCount == 0 {
		return false, false, "no metrics available"
	}

	avgLatency := totalLatency / float64(shardCount)
	avgCPU := totalCPU / float64(shardCount)

	if avgLatency > a.latencyThreshold {
		if currentShards < a.maxShards {
			return true, false, fmt.Sprintf("p99 latency %.3fs > threshold %.3fs", avgLatency, a.latencyThreshold)
		}
	}

	if avgCPU > a.cpuThreshold {
		if currentShards < a.maxShards {
			return true, false, fmt.Sprintf("CPU usage %.2f%% > threshold %.2f%%", avgCPU*100, a.cpuThreshold*100)
		}
	}

	if avgLatency < a.latencyThreshold*0.5 && avgCPU < a.cpuThreshold*0.5 {
		if currentShards > a.minShards {
			return false, true, fmt.Sprintf("low load: latency %.3fs, CPU %.2f%%", avgLatency, avgCPU*100)
		}
	}

	return false, false, "no scaling needed"
}

func (a *Autoscaler) RecordScaleEvent(action, reason string) {
	autoscalingEvents.WithLabelValues(action, reason).Inc()
	if action == "scale_up" {
		a.lastScaleUpTime = time.Now()
	} else if action == "scale_down" {
		a.lastScaleDownTime = time.Now()
	}
}

var (
	hashRing     = NewConsistentHashRing(100)
	autoscaler   = NewAutoscaler(hashRing, 1, 5)
	buildVersion = "1.0.0"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	autoscaler.AddShard("shard1", "http://shard1-leader:8080", []string{
		"http://shard1-leader:8080",
		"http://shard1-follower-1:8080",
		"http://shard1-follower-2:8080",
		"http://shard1-follower-3:8080",
	})

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			shouldScaleUp, shouldScaleDown, reason := autoscaler.CheckScaling()

			if shouldScaleUp {
				log.Printf("Autoscaling: Scale-up triggered - %s", reason)
				autoscaler.RecordScaleEvent("scale_up", reason)
				newShardName := fmt.Sprintf("shard%d", len(autoscaler.shards)+1)
				newLeaderURL := fmt.Sprintf("http://%s-leader:8080", newShardName)

				autoscaler.AddShard(newShardName, newLeaderURL, []string{newLeaderURL})
				log.Printf("Added new shard: %s", newShardName)
			}

			if shouldScaleDown {
				log.Printf("Autoscaling: Scale-down triggered - %s", reason)
				autoscaler.RecordScaleEvent("scale_down", reason)
				if len(autoscaler.shards) > 1 {
					var lastShard string
					for name := range autoscaler.shards {
						lastShard = name
					}
					if lastShard != "shard1" {
						autoscaler.RemoveShard(lastShard)
						log.Printf("Removed shard: %s", lastShard)
					}
				}
			}
		}
	}()

	http.HandleFunc("/data", metricsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter required", 400)
			return
		}

		targetURL := hashRing.GetNode(key)
		if targetURL == "" {
			http.Error(w, "no available shards", 503)
			return
		}

		proxyRequest(w, r, targetURL)
	}))

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	http.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": buildVersion})
	})

	http.HandleFunc("/shards", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		autoscaler.mu.Lock()
		shardsInfo := make([]ShardInfo, 0, len(autoscaler.shards))
		for _, shard := range autoscaler.shards {
			shardsInfo = append(shardsInfo, *shard)
		}
		autoscaler.mu.Unlock()

		json.NewEncoder(w).Encode(shardsInfo)
	})

	http.HandleFunc("/debug/hash", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter required", 400)
			return
		}

		node := hashRing.GetNode(key)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"key": key, "node": node})
	})

	http.HandleFunc("/metrics/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}

		var metrics struct {
			ShardName  string  `json:"shard_name"`
			LatencyP99 float64 `json:"latency_p99"`
			CPUUsage   float64 `json:"cpu_usage"`
		}

		if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		autoscaler.ReportMetrics(metrics.ShardName, metrics.LatencyP99, metrics.CPUUsage)
		w.WriteHeader(http.StatusOK)
	})

	http.Handle("/metrics", promhttp.Handler())

	log.Println("Coordinator started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		traceID := uuid.New().String()
		r.Header.Set("X-Trace-ID", traceID)

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		log.Printf(`{"level":"info", "trace_id":"%s", "msg":"Received request", "method":"%s", "path":"%s"}`,
			traceID, r.Method, r.URL.Path)

		next(rw, r)

		duration := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(r.Method, http.StatusText(rw.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method).Observe(duration)
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func proxyRequest(w http.ResponseWriter, r *http.Request, target string) {
	url := target + r.URL.String()

	log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Proxying request", "method":"%s", "target":"%s"}`,
		r.Header.Get("X-Trace-ID"), r.Method, target)

	req, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		log.Printf(`{"level":"error", "trace_id":"%s", "msg":"Failed to create request", "error":"%v"}`,
			r.Header.Get("X-Trace-ID"), err)
		http.Error(w, err.Error(), 500)
		return
	}

	traceID := r.Header.Get("X-Trace-ID")
	req.Header.Set("X-Trace-ID", traceID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf(`{"level":"error", "trace_id":"%s", "msg":"Shard unavailable", "error":"%v", "target":"%s"}`,
			traceID, err, target)
		http.Error(w, "Shard unavailable", 502)
		return
	}
	defer resp.Body.Close()

	log.Printf(`{"level":"debug", "trace_id":"%s", "msg":"Received response", "status":"%d"}`,
		traceID, resp.StatusCode)

	for name, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
