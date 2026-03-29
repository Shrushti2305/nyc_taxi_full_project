# GitHub Repository Files

## Files to Include ✅

### Root Level Configuration Files
- `docker-compose.yaml` - Airflow/Docker setup
- `requirements.txt` - Python dependencies
- `.gitignore` - Git ignore rules
- `.env.example` - Environment variables template (copy from .envairflow)

### Documentation Files
- `README.md` - Main documentation (UPDATED)
- `AIRFLOW_SETUP.md` - Airflow setup guide
- `AIRFLOW_TESTING.md` - Testing documentation
- `BLUE_GREEN_DEPLOYMENT.md` - Deployment guide
- `TASK2_SUMMARY.md` - Task summary

### Source Code - Python Scripts
- `load_data.py` - Data loading script
- `extract_log_sources.py` - Log extraction utility
- `generate_log_report.py` - Report generation
- `list_logs.py` - Log listing utility

### Airflow DAGs
- `dags/nyc_taxi_daily_pipeline.py` - Main DAG pipeline

### dbt Project
- `dbt/dbt_project.yml` - dbt project configuration
- `dbt/profiles.yml.example` - dbt profiles template
- `dbt/models/staging/*.sql` - Staging layer models
- `dbt/models/intermediate/*.sql` - Intermediate layer models
- `dbt/models/marts/*.sql` - Mart layer models
- `dbt/models/*/schema.yml` - Model schemas and tests
- `dbt/tests/*.sql` - Custom tests
- `dbt/macros/*.sql` - Custom macros
- `dbt/seeds/taxi_zone_lookup.csv` - Seed data

### Config Files
- `config/airflow.cfg` - Airflow configuration

### SQL Queries
- `queries/q1_top_zones_by_revenue.sql` - Query 1
- `queries/q2_hour_of_day_pattern.sql` - Query 2
- `queries/q3_consecutive_gap_analysis.sql` - Query 3

### Spark Scripts
- `spark/process_historical.py` - Historical data processing

---

## Files to Exclude (Add to .gitignore) ❌

### Data & Logs
- `data/` - Raw parquet files and DuckDB database
- `logs/` - Airflow execution logs
- `*.duckdb` - DuckDB database files

### Dependencies & Build
- `venv/` - Python virtual environment
- `__pycache__/` - Python cache
- `dbt/target/` - dbt compiled output
- `dbt/dbt_packages/` - dbt package dependencies

### Secrets & Local Config
- `.env` - Environment variables (use .env.example instead)
- `.envairflow` - Local Airflow config
- `.airflow/` - Local Airflow directory

### Generated Files
- `*.pyc` - Python compiled files
- `LOG_SOURCES_REPORT.txt` - Generated reports

### Plugins
- `plugins/` - Custom Airflow plugins (keep structure but empty)

---

## Setup Instructions for Cloners

See updated README.md for complete setup instructions.

