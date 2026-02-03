
## 🚀 JSON-Avro Optimization: Kafka Performance Benchmark

This project is an experimental study analyzing the trade-offs between infrastructure efficiency (Memory, Disk, Network) and system performance (Events Per Second) based on data serialization formats (JSON vs. Avro).

## 📌 Project Overview

As data scale grows, increasing storage costs and network traffic bottlenecks become critical issues. This project implements an optimized data pipeline using Apache Avro to solve these challenges and validates the results through a 40M message benchmark.

## 🛠 Tech Stack

- **Language:** Python 3.x
- **Messaging Queue:** Apache Kafka (via Docker)
- **Serialization:** Avro (fastavro), JSON
- **Data Manipulation:** pandas (with numeric downcasting)

## 📂 Project Structure

```text
Avro/
├── docker-compose.yml       # Kafka & Zookeeper services
├── memory_opt.py            # Memory-optimized loader using downcasting
├── kafka_producer.py        # Benchmark Base: Standard JSON-based producer
├── kafka_producer_avro.py   # Optimized Producer: Schema-based Avro serialization
├── kafka_consumer.py        # Benchmark Base: Standard JSON-based consumer
└── kafka_consumer_avro.py   # Optimized Consumer: Avro deserialization
```

## 📊 Key Results (40M Messages Benchmark)

*EPS = (Events Per Second)

| Metric | JSON (Base) | Avro (Optimized) | Efficiency |
|---|---|---|---|
| Total Messages | 39,679,618 | 40,000,000 | ~1:1 Scale Match |
| Total Disk Usage | 4.0 GB | 1.5 GB | 62.5% Reduction |
| Avg. Message Size | ~103 Bytes | ~38 Bytes | 2.7x More Compact |
| Producer EPS | ~160,000 | ~158,000 | Negligible Diff |
| Consumer EPS | ~161,000 | ~96,000 | CPU Overhead exists |

## 💡 Engineering Insights

1. By switching to Avro's binary serialization, the payload size was reduced from `103 bytes` to `38 bytes` per message.

2. This achieves a **62.5% reduction** in storage occupancy and network egress traffic, which is critical for cost-effective cloud infrastructure.

## 📝 Final Conclusion

This project demonstrates that data engineering is not just about speed, but about finding the optimal balance between resource utilization (Storage/Network) and processing performance (CPU).
