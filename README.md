# Real-Time E-Commerce Recommendation System
### Big Data Analytics — Mini Project 3

A real-time recommendation system built with **Apache Spark**, **Kafka**, and **Streamlit** using the Amazon Electronics Reviews dataset.

---

## Tech Stack

| Component | Technology |
|---|---|
| Cluster | Apache Spark 3.5.0 (master + 2 workers) |
| Message Broker | Apache Kafka 7.4.0 + Zookeeper |
| ML Algorithm | Spark MLlib — ALS (Alternating Least Squares) |
| Streaming | Spark Structured Streaming |
| Dashboard | Streamlit |
| Language | Python 3.10 |
| Infrastructure | Docker + docker-compose |

---

## Project Structure

```
spark-case-study/
├── docker-compose.yml          # Full cluster definition
├── Dockerfile                  # Spark + Python image
├── requirements.txt            # Python dependencies
├── dashboard.py                # Streamlit dashboard
├── notebooks/
│   ├── 01_als_training.ipynb   # Batch ML — ALS model training
│   ├── 02_streaming.ipynb      # Kafka consumer + window analytics
│   └── 03_integration.ipynb   # ML + streaming integration
├── scripts/
│   ├── load_data.py            # Download & preprocess Amazon dataset
│   └── kafka_producer.py       # Kafka event producer
└── retail_data/                # Mounted as /data/ inside containers
    ├── train_ratings.parquet
    ├── test_ratings.parquet
    ├── all_ratings.parquet
    ├── label_encoders.pkl
    ├── als_model/
    ├── user_top5_recs.parquet
    ├── integrated_recs.parquet
    ├── streaming_output/
    └── streaming_checkpoints/
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- At least **8GB RAM** allocated to Docker
- Ports **8080, 8081, 8082, 8888, 9092, 8501** free on your machine

---

## Step 1 — Start the Cluster

```bash
cd spark-case-study
docker compose up -d
```

> **Note:** All Python dependencies from `requirements.txt` are installed automatically during the Docker image build. No manual pip install is needed.

Wait ~30 seconds for all services to start, then verify:

```bash
docker compose ps
```

All 5 containers should show `Up`:
```
spark-master    Up
spark-worker-1  Up
spark-worker-2  Up
zookeeper       Up
kafka           Up
```

**Useful UIs:**
- Spark Master:  http://localhost:8080
- JupyterLab:   http://localhost:8888
- Worker 1:     http://localhost:8081
- Worker 2:     http://localhost:8082

---

## Step 2 — Load the Dataset

This downloads 2 million Amazon Electronics reviews, cleans them, encodes IDs, and saves train/test parquet files.

```bash
docker compose exec spark-master python3 /app/scripts/load_data.py
```

Expected output:
```
Loading Amazon Reviews 2023 - Electronics...
Total records parsed: 2,000,000
Records after cleaning: 1,998,xxx
Train: 1,600,000 | Test: 400,000
Done. Files saved to /data/
```

> **Note:** This may take 5–15 minutes depending on your internet speed. The file is ~2GB compressed.

---

## Step 3 — Train the ALS Model

Open JupyterLab at http://localhost:8888 and run **`notebooks/01_als_training.ipynb`** cell by cell.

This notebook will:
1. Load train/test parquet files
2. Train ALS model (rank=50, regParam=0.01, maxIter=20)
3. Evaluate RMSE on the test set
4. Tune hyperparameters if RMSE > 1.5
5. Save the model to `/data/als_model/`
6. Generate Top-5 recommendations per user → `/data/user_top5_recs.parquet`

> **Note:** Step 6 (recommendForAllUsers) takes 10–20 minutes on 2M records.

---

## Step 4 — Start the Streaming Consumer

Open a **new JupyterLab tab** and run **`notebooks/02_streaming.ipynb`** cell by cell.

This notebook will:
1. Connect to Kafka topic `user_events`
2. Start three streaming queries:
   - `console_output` — prints batches to notebook output
   - `parquet_sink` — writes to `/data/streaming_output/`
   - `alert_stream` — prints alerts for high-rated/active items

The last cell should show:
```
Active streaming queries: 3
  - parquet_sink      | status: Waiting for next trigger
  - alert_stream      | status: Waiting for next trigger
  - console_output    | status: Waiting for next trigger
```

> **Important:** Keep this notebook running. Do not shut down its kernel.

---

## Step 5 — Start the Kafka Producer

Open a **new terminal** and run:

```bash
# Stream all 400K test events (runs once, ~11 hours at 0.1s delay)
docker compose exec spark-master python3 /app/scripts/kafka_producer.py --delay 0.1

# OR: loop forever (recommended for dashboard demo)
docker compose exec spark-master bash -c "while true; do python3 /app/scripts/kafka_producer.py --delay 0.1; echo 'Restarting...'; sleep 2; done"

# OR: quick test with only 5000 events
docker compose exec spark-master python3 /app/scripts/kafka_producer.py --delay 0.05 --limit 5000
```

Expected output:
```
Loading data from /data/test_ratings.parquet ...
  400,000 events to stream → topic 'user_events' on kafka:9092
  [  0.1%] Sent 500/400,000 | errors: 0 | last user_id: 1733806
  ...
```

---

## Step 6 — Run the Integration Notebook

After **at least 30–60 seconds** of streaming (so parquet files are written), open a **new JupyterLab tab** and run **`notebooks/03_integration.ipynb`** cell by cell.

This notebook will:
1. Load the saved ALS model
2. Load Top-5 ALS recommendations
3. Load live streaming output from `/data/streaming_output/`
4. Deduplicate to get the latest trending score per item
5. Blend ALS scores with trending: `0.7 × ALS + 0.3 × trending_norm`
6. Re-rank Top-5 per user
7. Measure end-to-end latency (target: < 5 seconds)
8. Save results to `/data/integrated_recs.parquet`

---

## Step 7 — Launch the Dashboard

```bash
docker compose exec spark-master streamlit run /app/dashboard.py --server.port 8501 --server.address 0.0.0.0
```

Open http://localhost:8501 in your browser.

The dashboard auto-refreshes every 10 seconds and shows:
- **Streaming Metrics** — live KPI cards
- **Trending Items** — top 10 by trending score with progress bars
- **Alerts** — items with avg_rating > 4.5 or interactions > 50
- **Recommendations** — Top-5 per user with blended scores
- **User Activity** — interaction chart over time

---

## Running Order Summary

```
Terminal 1 (keep open):   docker compose up -d
Terminal 2 (keep open):   kafka_producer.py --delay 0.1 (loop)
Terminal 3 (keep open):   streamlit run dashboard.py

JupyterLab tab 1:         01_als_training.ipynb     (run once)
JupyterLab tab 2:         02_streaming.ipynb        (keep running)
JupyterLab tab 3:         03_integration.ipynb      (run after 60s of streaming)
```

---

## Troubleshooting

**Workers not registering (no resources available)**
```bash
# Check worker status
docker compose ps
docker compose logs spark-worker-1

# Restart workers
docker compose restart spark-worker-1 spark-worker-2
```

**Streaming batches are empty**
- Make sure the producer is running in the terminal
- Check the timestamp format in notebook 02 cell 6:
  ```python
  to_timestamp(col('data.timestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
  ```
- Delete checkpoints and restart:
  ```bash
  docker compose exec spark-master bash -c "rm -rf /data/streaming_checkpoints/*"
  ```

**Notebook 03 shows trending_score = 0 for all items**
- The streaming output may be empty — check:
  ```bash
  docker compose exec spark-master bash -c "ls -lth /data/streaming_output/ | head -5"
  ```
- Wait longer for the producer to send events and for parquet files to be written

**Dashboard not refreshing**
- Make sure the producer is still running (it may have finished all 400K events)
- Use the looping producer command from Step 5
- Check streaming output file timestamps are recent

**Port already in use**
```bash
docker compose down
docker compose up -d
```

**Out of memory**
- Increase Docker Desktop memory to 8GB+ in Settings → Resources
- Or reduce MAX_RECORDS in `load_data.py` to 500,000

---

## Stopping the System

```bash
# Stop all containers
docker compose down

# Stop and remove all data volumes (full reset)
docker compose down -v
```

---

## Team

| Name | Responsibilities |
|---|---|
| Student 1 | Data loading, ALS training, RMSE tuning, integration notebook, report |
| Student 2 | Kafka producer, streaming pipeline, alert system, dashboard |

**Course:** Big Data Analytics (DSAI 427) — Spring 2026