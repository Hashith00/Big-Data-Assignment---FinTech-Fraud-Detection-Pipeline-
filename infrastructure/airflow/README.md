Airflow DAG for Scenario 2 — FinTech Fraud ETL
--------------------------------------------

This folder contains an Airflow DAG that runs every 6 hours to:

- Extract validated (non-fraud) transactions from Kafka topic `valid-transactions` for the current 6-hour window and write Parquet to the warehouse.
- Extract ingress (all) transactions from Kafka topic `transactions` for the same 6-hour window.
- Compute a reconciliation report comparing total ingress vs validated amounts for that window.
- Produce an analytic report `Fraud Attempts by Merchant Category` from the existing Postgres fraud sink (`fraud_transactions`) for the same window.

Files:

- `dags/fraud_etl_dag.py` — the DAG implementation.
- `requirements.txt` — Python libs required by the DAG tasks.

Quick run (recommended for local dev):

1. Install Airflow (use a venv) and required packages

```bash
python -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install apache-airflow
pip install -r infrastructure/airflow/requirements.txt
```

2. Start Airflow standalone (simple dev server)

```bash
AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/infrastructure/airflow/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow standalone
```

Note: On Windows, set the `AIRFLOW__CORE__DAGS_FOLDER` env var via PowerShell or CMD before running `airflow standalone`.

3. Ensure Kafka is reachable from the Airflow environment. By default the DAG uses `kafka:9092` (matching the project's docker-compose network). If you run Airflow outside Docker, set `KAFKA_BOOTSTRAP` env var to `localhost:29092`.

4. Ensure Postgres is reachable from the Airflow environment. The merchant-category report reads from `fraud_transactions` using `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, and `PG_PASSWORD`. The defaults match the existing `postgres` service in `docker-compose.yml`.

5. Output locations (defaults):

- Warehouse Parquet files: `/opt/airflow/warehouse`
- Reports: `/opt/airflow/reports`

You can override these via env vars `AIRFLOW_WAREHOUSE` and `AIRFLOW_REPORTS`.

Docker-compose integration (optional):

If you prefer to run Airflow inside Docker, add an Airflow service and mount `infrastructure/airflow/dags` into the container's DAGs folder and install the requirements into the image or use the official images' `AIRFLOW__CORE__DAGS_FOLDER` mapping. For a quick start, run Airflow locally with `airflow standalone` and point it at the DAGs folder.
