echo "Starting high load test to demonstrate autoscaling..."
echo "This will run for 2 minutes generating mixed workload"
echo "Monitor shard count in another terminal with:"
echo "   watch -n 1 'curl -s http://localhost:8080/shards | python3 -m json.tool'"
echo ""

END_TIME=$((SECONDS + 120)) 
REQUEST_COUNT=0

while [ $SECONDS -lt $END_TIME ]; do
    KEY="load_key_$((RANDOM % 1000))"
    VALUE="load_value_$(date +%s)_$RANDOM"

    if [ $((RANDOM % 10)) -lt 7 ]; then
        curl -s "http://localhost:8080/data?key=$KEY" > /dev/null &
    else
        curl -X POST -d "$VALUE" "http://localhost:8080/data?key=$KEY" > /dev/null &
    fi

    REQUEST_COUNT=$((REQUEST_COUNT + 1))
    sleep 0.005
done

wait
echo "Total requests sent: $REQUEST_COUNT"
