# Real-Time Ride-Hailing Demand Pipeline (Lambda Architecture)
A complete, end-to-end real-time stream processing pipeline simulating a ride-hailing backend (like Grab or Gojek). It processes raw GPS ride requests, computes H3 geospatial indices, and outputs to both a low-latency serving layer (Hot Path) and an S3-compatible Data Lake (Cold Path).

## Architecture Overview

This project implements a Lambda Architecture:
1. **Ingestion:** A Python producer replays historical NYC TLC Parquet data as a live chronological stream into **Redpanda (Kafka)**.
2. **Stream Processing:** **Apache Spark Structured Streaming** consumes the raw JSON events, maps raw coordinates to **Uber H3 Hexagons** (resolution 8), and computes 5-minute tumbling windows using event-time watermarking to handle late-arriving data.
3. **Hot Path (Serving):** Aggregated demand metrics are upserted into **Redis** with a 10-minute TTL. A **FastAPI** backend serves this memory cache to a **Streamlit/PyDeck** frontend, rendering a live 3D map updated every 3 seconds.
4. **Cold Path (Data Lake):** Simultaneously, Spark sinks the enriched, unaggregated event stream as partitioned Parquet files into an S3-compatible **LocalStack** bucket for historical batch analytics.

## Tech Stack
* **Language:** Python 3.11
* **Streaming Engine:** Apache Spark Structured Streaming (PySpark 3.5.1)
* **Message Broker:** Redpanda (C++ Kafka alternative)
* **Serving Layer:** Redis, FastAPI
* **Data Lake:** LocalStack (AWS S3 Emulator)
* **Geospatial:** Uber H3, GeoPandas, PyDeck
* **Infrastructure:** Docker Compose

## Quickstart Guide

### Prerequisites
* Docker and Docker Compose installed.
* Python 3.11+
* OpenJDK 17 (required for PySpark)

```bash
# Clone the repository
git clone [https://github.com/YOUR_USERNAME/ride-streaming-pipeline.git](https://github.com/YOUR_USERNAME/ride-streaming-pipeline.git)
cd ride-streaming-pipeline

# Install dependencies
pip install -r requirements.txt