# Airflow Setup Guide

## Quick Start

### 1. Initialize Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
```

### 2. Create Admin User
```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password airflow
```

### 3. Start Airflow Services (separate terminals)

**Terminal 1 - Webserver:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
# Visit http://localhost:8080
```

**Terminal 2 - Scheduler:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

---

## DAG Configuration

### Environment Variables
Create `.env` file in project root:
```bash
export DBT_DIR="<project_root>/dbt"
export DBT_PROFILES_DIR="$HOME/.dbt"
export AIRFLOW_ALERT_EMAIL="your-email@example.com"
export DUCKDB_PATH="<project_root>/data/nyc_taxi.duckdb"
```

Load before starting Airflow:
```bash
source .env
```

### DAG Details
- **DAG ID**: `nyc_taxi_daily_pipeline`
- **Schedule**: Daily at 02:00 UTC (`0 2 * * *`)
- **Backfill**: Enabled via `catchup=True`
- **Retries**: 2 attempts with 5-minute delays
- **Email Alerts**: On failure (configure recipient in `AIRFLOW_ALERT_EMAIL`)

---

## DAG Variables (Optional)

In Airflow UI → Admin → Variables:
```json
{
  "data_dir": "../data",
  "dbt_profiles_dir": "~/.dbt",
  "alert_email": "data-team@company.com"
}
```

In DAG code:
```python
{{ var.value.get("data_dir", "../data") }}
```

---

## Airflow Connections (No Hardcoded Credentials)

### For Email Alerts
In Airflow UI → Admin → Connections → Create:
```
Conn Id:      email_alerts
Conn Type:    Email
Host:         smtp.gmail.com
Port:         587
Login:        your-email@gmail.com
Password:     your-app-password
Extra:        {"email_from": "your-email@gmail.com"}
```

### For DuckDB (Optional)
```
Conn Id:      duckdb_conn
Conn Type:    DuckDB
Host:         <project_root>/data/nyc_taxi.duckdb
```

---

## Backfill Example

### Single Date Backfill
```bash
airflow dags backfill \
  --dag-id nyc_taxi_daily_pipeline \
  --start-date 2023-01-01 \
  --end-date 2023-01-05
```

### With Retry
```bash
airflow dags backfill \
  --dag-id nyc_taxi_daily_pipeline \
  --start-date 2023-01-01 \
  --end-date 2023-01-05 \
  --reset-dagruns
```

---

## Testing DAG Locally

### Validate DAG Syntax
```bash
python -m py_compile dags/nyc_taxi_daily_pipeline.py
```

### List all DAGs
```bash
airflow dags list
```

### Test Task Execution (without Scheduler)
```bash
airflow tasks test nyc_taxi_daily_pipeline check_source_freshness 2023-01-01
```

### Run all tasks in DAG (dry-run)
```bash
airflow dags test nyc_taxi_daily_pipeline 2023-01-01
```

---

## Monitoring & Logs

### View Task Logs
```bash
# Webserver → DAG → Task Instance → View Logs
# Or via CLI:
airflow tasks logs nyc_taxi_daily_pipeline run_dbt_tests 2023-01-01T02:00:00
```

### Check DAG Status
```bash
airflow dags state nyc_taxi_daily_pipeline 2023-01-01T02:00:00
```

### List all DAG Runs
```bash
airflow dags list-runs --dag-id nyc_taxi_daily_pipeline
```

---

## Production Deployment (Docker)

### Dockerfile
```dockerfile
FROM apache/airflow:2.7.0-python3.11

USER root
RUN apt-get update && apt-get install -y git

USER airflow

# Copy DAG
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
```

### docker-compose.yml
```yaml
version: '3.8'
services:
  airflow:
    image: nyc-taxi-airflow:latest
    environment:
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - DBT_DIR=/opt/airflow/dags/../dbt
      - AIRFLOW_ALERT_EMAIL=admin@example.com
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dbt:/opt/airflow/dbt
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    depends_on:
      - postgres

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
```

---

## Troubleshooting

### Issue: DAG not appearing in Airflow UI
**Solution:**
1. Ensure DAG file is in `AIRFLOW_HOME/dags/`
2. Restart scheduler: `airflow scheduler`
3. Check DAG syntax: `python -m py_compile dags/nyc_taxi_daily_pipeline.py`

### Issue: Task fails with "dbt not found"
**Solution:**
Ensure `DBT_DIR` environment variable is set and dbt is installed:
```bash
export DBT_DIR="/path/to/dbt"
which dbt
```

### Issue: Email alerts not sending
**Solution:**
1. Configure Email connection in Airflow UI
2. Set `AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com` environment variable
3. Test: `airflow tasks test ... notify_success`

### Issue: BashOperator failing on Windows
**Solution:**
Use WSL2 or Docker for Airflow. Windows Git Bash may have path issues with BashOperator.

---

## Best Practices

✅ **Use Environment Variables**: Never hardcode credentials  
✅ **Set Timeouts**: Add `execution_timeout=3600` to operators  
✅ **Handle Failures**: Use `trigger_rule='all_done'` for cleanup tasks  
✅ **Monitor**: Set up alerting on task failures  
✅ **Test**: Use `airflow tasks test` before production  
✅ **Document**: Add descriptions to DAG and tasks  
✅ **Version Control**: Track `dags/` and `requirements.txt`

---

## References
- [Airflow Official Docs](https://airflow.apache.org/)
- [Airflow Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/index.html)
- [DBT in Airflow](https://docs.getdbt.com/docs/running-a-dbt-project/orchestration)
