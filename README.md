# Real-Time Log Analytics Pipeline

Streams synthetic API logs through Kafka, aggregates them with Spark Structured Streaming, and visualizes live metrics on a Dash dashboard.

**Flow:** Log Producer → Kafka (`logging_info`) → Spark Streaming → Kafka (`agg_logging_info`) → Consumer → DynamoDB → Dash UI

## Stack
Kafka · Zookeeper · PySpark Structured Streaming · DynamoDB Local · Dash

## Setup

```bash
# 1. Set shared dir in .env
echo 'HOST_SHARED_DIR=/path/to/shared/dir' > .env

# 2. Start infra
docker-compose up -d

# 3. Get Jupyter token
docker exec -it spark-jupyter-portfolio jupyter server list

# 4. Create Kafka topics
python kafka/kafka_topic.py

# 5. Run spark_portfolio.ipynb in Jupyter Lab (all cells, end with query.awaitTermination())

# 6. Start the log producer

# 7. Start the aggregated-log consumer
python kafka/spark_kafka_consumer.py

# 8. Launch the dashboard
python ui/ui_prod.py   # http://127.0.0.1:8050
```

## Notes
- `.start()` runs async — always call `query.awaitTermination()` to keep the stream alive.
- DynamoDB Admin UI: `http://localhost:8001`
- 1-minute windows + 1-minute watermark → allow ~1–2 min before data appears.

## License
MIT
