#!/usr/bin/env python3

import requests
import time
import threading
import random
import statistics
import json
from datetime import datetime

# Configuration
COORDINATOR_URL = "http://localhost:8080/data"
TOTAL_REQUESTS = 5000  
CONCURRENCY = 50       
DURATION = 300  # 5 minutes

latencies = []
lock = threading.Lock()
start_time = time.time()

metrics = {
    "requests": [],
    "shard_changes": [],
    "autoscaling_events": []
}

def worker(worker_id):
    """Worker function that generates load"""
    global latencies
    
    while time.time() - start_time < DURATION:
        key = f"load_test_key_{random.randint(1, 1000)}"
        value = f"load_test_value_{worker_id}_{random.randint(1, 10000)}"
        
        if random.random() > 0.3:  
            url = f"{COORDINATOR_URL}?key={key}"
            method = "GET"
        else:
            url = f"{COORDINATOR_URL}?key={key}"
            method = "POST"
        
        try:
            start_req = time.time()
            
            if method == "GET":
                resp = requests.get(url)
            else:
                resp = requests.post(url, data=value)
            
            duration = (time.time() - start_req) * 1000  
            
            with lock:
                latencies.append(duration)
                metrics["requests"].append({
                    "timestamp": time.time(),
                    "method": method,
                    "latency": duration,
                    "status": resp.status_code
                })
            
            if random.random() < 0.01: 
                check_shard_status()
                check_autoscaling_metrics()
                
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
            time.sleep(0.1)

def check_shard_status():
    """Check current shard status from coordinator"""
    try:
        resp = requests.get("http://localhost:8080/shards")
        if resp.status_code == 200:
            shards = resp.json()
            timestamp = time.time()
            
            with lock:
                metrics["shard_changes"].append({
                    "timestamp": timestamp,
                    "shard_count": len(shards),
                    "shards": [s["name"] for s in shards]
                })
            
            print(f"[{datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}] "
                  f"Shard count: {len(shards)}, Shards: {[s['name'] for s in shards]}")
    except Exception as e:
        print(f"Error checking shard status: {e}")

def check_autoscaling_metrics():
    """Check autoscaling metrics from coordinator"""
    try:
        resp = requests.get("http://localhost:8080/metrics")
        if resp.status_code == 200:
            lines = resp.text.split('\n')
            for line in lines:
                if line.startswith('coordinator_autoscaling_events_total'):
                    parts = line.split()
                    if len(parts) >= 3:
                        labels_start = line.find('{')
                        labels_end = line.find('}')
                        if labels_start != -1 and labels_end != -1:
                            labels = line[labels_start+1:labels_end]
                            label_parts = labels.split(',')
                            labels_dict = {}
                            for lp in label_parts:
                                if '=' in lp:
                                    k, v = lp.split('=', 1)
                                    labels_dict[k.strip()] = v.strip('"')
                            
                            if 'action' in labels_dict and 'reason' in labels_dict:
                                timestamp = time.time()
                                with lock:
                                    metrics["autoscaling_events"].append({
                                        "timestamp": timestamp,
                                        "action": labels_dict["action"],
                                        "reason": labels_dict["reason"]
                                    })
                                
                                print(f"[{datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}] "
                                      f"Autoscaling event: {labels_dict['action']} - {labels_dict['reason']}")
    except Exception as e:
        print(f"Error checking autoscaling metrics: {e}")

def main():
    print("Starting load test...")
    print(f"Duration: {DURATION} seconds")
    print(f"Concurrency: {CONCURRENCY} workers")
    print(f"Target: {COORDINATOR_URL}")
    print("Press Ctrl+C to stop early\n")
    
    threads = []
    for i in range(CONCURRENCY):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    
    check_shard_status()
    
    try:
        while time.time() - start_time < DURATION:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nLoad test interrupted by user")
    
    for t in threads:
        t.join(timeout=1)
    
    generate_report()

def generate_report():
    """Generate a summary report of the load test"""
    print("\n" + "="*60)
    print("LOAD TEST REPORT")
    print("="*60)
    
    total_time = time.time() - start_time
    request_count = len(latencies)
    
    if request_count > 0:
        avg_latency = statistics.mean(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies)
        p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
        throughput = request_count / total_time
        
        print(f"Total Time:     {total_time:.2f} s")
        print(f"Total Requests: {request_count}")
        print(f"Throughput:     {throughput:.2f} Ops/sec")
        print(f"Avg Latency:    {avg_latency:.2f} ms")
        print(f"P95 Latency:    {p95_latency:.2f} ms")
        print(f"P99 Latency:    {p99_latency:.2f} ms")
    
    if metrics["shard_changes"]:
        print(f"\nShard Changes:")
        initial_count = metrics["shard_changes"][0]["shard_count"]
        final_count = metrics["shard_changes"][-1]["shard_count"]
        print(f"  Initial shards: {initial_count}")
        print(f"  Final shards:   {final_count}")
        print(f"  Change:         {final_count - initial_count}")
        
        if metrics["autoscaling_events"]:
            print(f"\nAutoscaling Events:")
            for event in metrics["autoscaling_events"]:
                action = event["action"]
                reason = event["reason"]
                print(f"  {action.upper()}: {reason}")
    
    with open('load_test_metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\nDetailed metrics saved to load_test_metrics.json")
    print("="*60)

if __name__ == "__main__":
    main()