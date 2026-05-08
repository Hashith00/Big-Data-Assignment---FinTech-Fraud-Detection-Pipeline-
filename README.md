# FinTech Fraud Detection Pipeline

This repository implements an academic assignment: a simulated FinTech fraud detection pipeline built to demonstrate real-time and batch processing concepts. The project provides runnable, containerized components (see `infrastructure/docker-compose.yml`) and supporting scripts under `Data/` and `spark-app/` that together simulate transaction generation, ingestion, stream processing, and scheduled batch analytics.

The pipeline combines event ingestion, stream-based detection, and periodic batch reporting. A Python generator produces sample transactions consumed by the ingestion layer; Apache Kafka (simulated locally via Docker Compose) acts as the event bus; Apache Spark Structured Streaming executes deterministic fraud rules; Apache Airflow orchestrates periodic batch jobs; and PostgreSQL stores flagged alerts for audit. The goal is educational: to show how event-time aware streaming, low-latency alerting, and auditable batch reconciliation work together in a reproducible local setup.

See `PROJECT_REPORT.md` for architecture and design details, and `infrastructure/` for instructions to launch the stack locally.
