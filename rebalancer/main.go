package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	rebalancingOperations = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "rebalancer_operations_total",
			Help: "Total number of rebalancing operations",
		},
	)
	keysMigrated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "rebalancer_keys_migrated_total",
			Help: "Total number of keys migrated",
		},
	)
	migrationDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "rebalancer_migration_duration_seconds",
			Help:    "Duration of migration operations",
			Buckets: prometheus.DefBuckets,
		},
	)
	rebalancingInProgress = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "rebalancer_in_progress",
			Help: "Whether rebalancing is currently in progress (1=yes, 0=no)",
		},
	)
)

func init() {
	prometheus.MustRegister(rebalancingOperations)
	prometheus.MustRegister(keysMigrated)
	prometheus.MustRegister(migrationDuration)
	prometheus.MustRegister(rebalancingInProgress)
}

type MigrationTask struct {
	SourceShard string   `json:"source_shard"`
	TargetShard string   `json:"target_shard"`
	Keys        []string `json:"keys"`
	Status      string   `json:"status"`
}

type Rebalancer struct {
	mu               sync.Mutex
	coordinatorURL   string
	migrationTasks   []MigrationTask
	currentTaskIndex int
	stopChan         chan struct{}
	buildVersion     string
}

func NewRebalancer(coordinatorURL string) *Rebalancer {
	return &Rebalancer{
		coordinatorURL: coordinatorURL,
		stopChan:       make(chan struct{}),
		buildVersion:   "1.0.0",
	}
}

func (r *Rebalancer) Start() {
	go r.monitorShardChanges()
	go r.processMigrationTasks()
}

func (r *Rebalancer) Stop() {
	close(r.stopChan)
}

func (r *Rebalancer) monitorShardChanges() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	var previousShards []string

	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			shards, err := r.getCurrentShards()
			if err != nil {
				log.Printf("Failed to get current shards: %v", err)
				continue
			}

			if !r.shardsChanged(previousShards, shards) {
				continue
			}

			log.Printf("Detected shard configuration change. Previous: %v, Current: %v",
				previousShards, shards)

			if len(previousShards) > 0 && len(shards) > 0 && len(previousShards) != len(shards) {
				log.Printf("Shard count changed from %d to %d, triggering rebalancing",
					len(previousShards), len(shards))
				r.triggerRebalancing(previousShards, shards)
			}

			previousShards = shards
		}
	}
}

func (r *Rebalancer) shardsChanged(old, new []string) bool {
	if len(old) != len(new) {
		return true
	}

	oldMap := make(map[string]bool)
	for _, shard := range old {
		oldMap[shard] = true
	}

	for _, shard := range new {
		if !oldMap[shard] {
			return true
		}
	}

	return false
}

func (r *Rebalancer) getCurrentShards() ([]string, error) {
	resp, err := http.Get(r.coordinatorURL + "/shards")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shards: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coordinator returned status: %d", resp.StatusCode)
	}

	var shardsInfo []struct {
		Name      string   `json:"name"`
		LeaderURL string   `json:"leader_url"`
		Replicas  []string `json:"replicas"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&shardsInfo); err != nil {
		return nil, fmt.Errorf("failed to decode shards response: %v", err)
	}

	var shardNames []string
	for _, shard := range shardsInfo {
		shardNames = append(shardNames, shard.Name)
	}

	return shardNames, nil
}

func (r *Rebalancer) triggerRebalancing(oldShards, newShards []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldMap := make(map[string]bool)
	for _, shard := range oldShards {
		oldMap[shard] = true
	}

	newMap := make(map[string]bool)
	for _, shard := range newShards {
		newMap[shard] = true
	}

	var addedShards []string
	for _, shard := range newShards {
		if !oldMap[shard] {
			addedShards = append(addedShards, shard)
		}
	}

	var removedShards []string
	for _, shard := range oldShards {
		if !newMap[shard] {
			removedShards = append(removedShards, shard)
		}
	}

	log.Printf("Rebalancing triggered: added shards: %v, removed shards: %v",
		addedShards, removedShards)

	if len(addedShards) > 0 {
		keysToMigrate, err := r.determineKeysToMigrate(addedShards)
		if err != nil {
			log.Printf("Failed to determine keys to migrate: %v", err)
			return
		}

		for _, addedShard := range addedShards {
			if keys, ok := keysToMigrate[addedShard]; ok && len(keys) > 0 {
				task := MigrationTask{
					SourceShard: "existing",
					TargetShard: addedShard,
					Keys:        keys,
					Status:      "pending",
				}
				r.migrationTasks = append(r.migrationTasks, task)
				log.Printf("Created migration task: %d keys from existing shards to %s",
					len(keys), addedShard)
			}
		}

		rebalancingOperations.Inc()
	}
}

func (r *Rebalancer) determineKeysToMigrate(addedShards []string) (map[string][]string, error) {
	keysToMigrate := make(map[string][]string)

	totalKeys := 100
	keysPerShard := totalKeys / len(addedShards)

	for i, shard := range addedShards {
		var keys []string
		start := i * keysPerShard
		end := start + keysPerShard
		if i == len(addedShards)-1 {
			end = totalKeys
		}

		for j := start; j < end; j++ {
			keys = append(keys, fmt.Sprintf("migration_key_%d", j))
		}

		keysToMigrate[shard] = keys
	}

	return keysToMigrate, nil
}

func (r *Rebalancer) processMigrationTasks() {
	for {
		select {
		case <-r.stopChan:
			return
		default:
			r.mu.Lock()

			if r.currentTaskIndex >= len(r.migrationTasks) {
				r.mu.Unlock()
				time.Sleep(5 * time.Second)
				continue
			}

			task := &r.migrationTasks[r.currentTaskIndex]

			if task.Status == "pending" {
				task.Status = "in_progress"
				rebalancingInProgress.Set(1)

				go func(task *MigrationTask) {
					startTime := time.Now()
					defer func() {
						migrationDuration.Observe(time.Since(startTime).Seconds())
						rebalancingInProgress.Set(0)
					}()

					log.Printf("Starting migration of %d keys from %s to %s",
						len(task.Keys), task.SourceShard, task.TargetShard)

					success := r.migrateKeys(task)

					r.mu.Lock()
					if success {
						task.Status = "completed"
						keysMigrated.Add(float64(len(task.Keys)))
						log.Printf("Completed migration of %d keys to %s", len(task.Keys), task.TargetShard)
					} else {
						task.Status = "failed"
						log.Printf("Failed migration of keys to %s", task.TargetShard)
					}
					r.currentTaskIndex++
					r.mu.Unlock()
				}(task)
			}

			r.mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}
}

func (r *Rebalancer) migrateKeys(task *MigrationTask) bool {
	log.Printf("Simulating migration of %d keys to %s", len(task.Keys), task.TargetShard)

	time.Sleep(time.Duration(len(task.Keys)/10) * time.Second)

	if rand.Float64() < 0.1 {
		return false
	}

	return true
}

func main() {
	coordinatorURL := os.Getenv("COORDINATOR_URL")
	if coordinatorURL == "" {
		coordinatorURL = "http://coordinator:8080"
	}

	rebalancer := NewRebalancer(coordinatorURL)
	rebalancer.Start()
	defer rebalancer.Stop()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	http.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": rebalancer.buildVersion})
	})

	http.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		rebalancer.mu.Lock()
		defer rebalancer.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rebalancer.migrationTasks)
	})

	http.Handle("/metrics", promhttp.Handler())

	log.Println("Rebalancer started on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
