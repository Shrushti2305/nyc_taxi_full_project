# NYC Taxi Data Engineering Pipeline

A production-ready end-to-end data engineering pipeline using Apache Airflow, dbt, DuckDB, and PySpark to process NYC Taxi 2023 dataset (~38M rows). The pipeline orchestrates data loading, transformation, quality testing, and analytics on a daily basis.

## 🎯 Project Overview

This project demonstrates a complete modern data engineering workflow:
- **Data Ingestion**: Load raw Parquet files into DuckDB
- **Transformation**: Multi-layer dbt pipeline (staging → intermediate → marts)
- **Orchestration**: Apache Airflow DAG for daily scheduling
- **Testing**: 64+ data quality tests (dbt tests)
- **Analytics**: SQL queries for business insights
- **Scalability**: PySpark for historical large-scale processing
- **Docker**: Fully containerized for easy deployment

---

## 🏗️ Architecture

```
Raw Parquet Files (38M rows)
    ↓
DuckDB (raw_yellow_trips)
    ↓
dbt Staging Layer (data cleaning & standardization)
    ├── stg_yellow_trips (trip records)
    └── stg_taxi_zones (zone dimensions)
    ↓
dbt Intermediate Layer (business logic & enrichment)
    └── int_trips_enriched (filtered, deduplicated trips)
    ↓
dbt Mart Layer (analytics-ready tables)
    ├── fct_trips (fact table: enriched trip data)
    ├── dim_zones (dimension: zone information)
    ├── agg_daily_revenue (aggregated daily metrics)
    └── agg_zone_performance (zone performance metrics)
    ↓
Data Quality Tests (64 tests)
    ↓
Analytics SQL Queries & Reporting
    ↓
Airflow DAG Orchestration (daily execution)
    ↓
PySpark Historical Processing (batch processing)
```

---

## 📋 Prerequisites

- **Python 3.11+**
- **Docker & Docker Compose** (for Airflow)
- **Git**
- **8GB+ RAM** recommended for Airflow
- **NYC Taxi 2023 Parquet Files** (download from [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page))

---

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/Shrushti2305/nyc_taxi_full_project
cd nyc_taxi_full_project
```

### 2. Create Virtual Environment
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
cp .env.example .env
# Edit .env with your settings if needed
```

### 5. Download & Place Data
Download NYC Taxi Parquet files and place them in `data/`:
```
data/
├── yellow_tripdata_2023-01.parquet
├── yellow_tripdata_2023-02.parquet
├── ...
└── yellow_tripdata_2023-12.parquet
```

### 6. Load Raw Data into DuckDB
```bash
python load_data.py
```

### 7. Run dbt Transformation Pipeline
```bash
cd dbt

# Install dbt seed data (taxi zone mappings)
dbt seed

# Run transformation models
dbt run

# Run data quality tests (64 tests)
dbt test

# Generate documentation
dbt docs generate
```

### 8. Start Airflow (Docker)
```bash
docker-compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

# Wait 30 seconds for services to start
sleep 30

# Access Airflow UI
# Open: http://localhost:8080
# Username: airflow
# Password: airflow
```

### 9. Trigger DAG
In Airflow UI:
1. Navigate to DAGs
2. Find `nyc_taxi_daily_pipeline`
3. Click "Trigger DAG" button
4. Monitor execution in UI

---

## 📁 Project Structure

```
nyc_taxi_full_project/
│
├── 📄 docker-compose.yaml          # Airflow + PostgreSQL + Redis setup
├── 📄 requirements.txt             # Python dependencies
├── 📄 .env.example                 # Environment template (rename to .env)
├── 📄 .gitignore                   # Git ignore rules
│
├── 📂 dags/                        # Airflow DAGs
│   └── nyc_taxi_daily_pipeline.py  # Main orchestration DAG
│
├── 📂 dbt/                         # Data transformation layer
│   ├── dbt_project.yml             # dbt configuration
│   ├── profiles.yml                # DuckDB connection profile
│   ├── models/
│   │   ├── staging/                # Layer 1: Raw → Clean
│   │   │   ├── stg_yellow_trips.sql
│   │   │   ├── stg_taxi_zones.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/           # Layer 2: Business logic
│   │   │   ├── int_trips_enriched.sql
│   │   │   └── schema.yml
│   │   └── marts/                  # Layer 3: Analytics-ready
│   │       ├── fct_trips.sql
│   │       ├── dim_zones.sql
│   │       ├── agg_daily_revenue.sql
│   │       ├── agg_zone_performance.sql
│   │       └── schema.yml
│   ├── tests/                      # Data quality tests
│   │   ├── test_total_amount.sql
│   │   ├── test_enriched_zones_populated.sql
│   │   ├── test_stg_zones_valid_ids.sql
│   │   ├── test_zone_performance_rank_validity.sql
│   │   └── ...
│   ├── macros/                     # Custom test macros
│   │   ├── test_trip_duration.sql
│   │   └── aggregation_tests.sql
│   └── seeds/
│       └── taxi_zone_lookup.csv    # Reference data
│
├── 📂 queries/                     # Analytics SQL queries
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   └── q3_consecutive_gap_analysis.sql
│
├── 📂 spark/                       # PySpark scripts
│   └── process_historical.py       # Historical batch processing
│
├── 📂 config/                      # Configuration files
│   └── airflow.cfg                 # Airflow settings
│
├── 📂 data/                        # Data directory (in .gitignore)
│   └── nyc_taxi.duckdb             # DuckDB database
│
├── 📂 logs/                        # Airflow logs (in .gitignore)
│
└── 📄 README.md                    # This file
```

---

## 🔄 DAG Pipeline Flow

The Airflow DAG (`nyc_taxi_daily_pipeline.py`) executes the following sequence:

```
1. check_source_freshness
   └─→ Validates source data availability
   
2. load_raw_data
   └─→ Loads Parquet files into DuckDB raw table
   
3. run_dbt_staging
   └─→ Executes dbt staging models (data cleaning)
   
4. run_dbt_intermediate
   └─→ Executes dbt intermediate models (enrichment)
   
5. run_dbt_marts
   └─→ Executes dbt mart models (analytics tables)
   
6. run_dbt_tests
   └─→ Runs 64+ data quality tests
   
7. notify_success
   └─→ Logs pipeline completion summary
```

**Schedule**: Daily at 02:00 UTC (configurable in DAG)
**Executor**: LocalExecutor (runs tasks in parallel)
**Max Retries**: 2 retries per task with 5-minute delays

---

## 🗄️ Database Schema

### Raw Layer
- **raw_yellow_trips**: All columns from source Parquet files

### Staging Layer
- **stg_yellow_trips**: Cleaned trip records with standardized column names
- **stg_taxi_zones**: Zone reference data

### Intermediate Layer
- **int_trips_enriched**: Filtered trips with enrichment (duration calculations, zone joins)

### Mart Layer
- **fct_trips** (fact): Complete enriched trip data with ~20 dimensions
- **dim_zones**: Zone master data with borough and service zone
- **agg_daily_revenue**: Daily revenue metrics by date
- **agg_zone_performance**: Zone performance rankings and metrics

---

## 🧪 Data Quality Tests

The project includes 64+ data quality tests covering:

### Uniqueness & Nullability
- Primary key uniqueness
- Required field non-null checks

### Referential Integrity
- Foreign key relationships between tables
- Valid zone IDs

### Business Logic
- Trip duration within valid range (1-180 minutes)
- Positive revenue amounts
- Positive trip counts
- Total amount > fare amount + tip

### Derived Metrics
- Zone performance rank validity
- Revenue aggregation accuracy
- Fare amount consistency

**Run tests**: `dbt test` (executes all tests)

---

## 📊 Analytics Queries

Three sample analytics queries in `queries/`:

1. **q1_top_zones_by_revenue.sql** — Top 10 zones by total revenue
2. **q2_hour_of_day_pattern.sql** — Pickup patterns by hour
3. **q3_consecutive_gap_analysis.sql** — Consecutive trip gaps analysis

Run manually:
```bash
duckdb data/nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
```

---

## 🐳 Docker & Airflow Configuration

Airflow uses **LocalExecutor** for task execution and is orchestrated with Docker Compose.

### Services
- **airflow-scheduler**: Schedules and monitors DAGs
- **airflow-apiserver**: REST API for Airflow
- **airflow-dag-processor**: Processes DAG definitions
- **postgres**: Metadata database
- **redis**: Message broker (optional, for Celery)

### Access Airflow UI
- **URL**: http://localhost:8080
- **Default Username**: airflow
- **Default Password**: airflow

### View Logs
```bash
docker-compose logs airflow-scheduler -f
```

### Stop Services
```bash
docker-compose down
```

---

## 🚀 Advanced Usage

### Run dbt in Dry-Run Mode
```bash
cd dbt
dbt run --dry-run  # Shows SQL without executing
```

### Generate dbt Documentation
```bash
cd dbt
dbt docs generate
dbt docs serve  # Serves docs on http://localhost:8000
```

### Execute Specific dbt Model
```bash
cd dbt
dbt run --select stg_yellow_trips  # Run only this model
```

### PySpark Historical Processing
```bash
python spark/process_historical.py
```

### Query DuckDB Directly
```bash
duckdb data/nyc_taxi.duckdb

# In DuckDB prompt:
SELECT * FROM fct_trips LIMIT 10;
SELECT COUNT(*) FROM fct_trips;
```

---

## 🔧 Troubleshooting

### Airflow DAG Not Appearing
```bash
# Restart scheduler
docker-compose restart airflow-scheduler

# Check DAG syntax
cd dags && python -m py_compile nyc_taxi_daily_pipeline.py
```

### dbt Compilation Errors
```bash
cd dbt
dbt parse  # Check syntax
dbt compile  # Full compilation
```

### DuckDB Connection Issues
```bash
# Verify database exists
ls -la data/nyc_taxi.duckdb

# Reinitialize if corrupted
rm data/nyc_taxi.duckdb
python load_data.py
```

### Docker Container Exit Codes
```bash
# View container logs
docker-compose logs airflow-scheduler

# Rebuild without cache
docker-compose build --no-cache
docker-compose up -d
```

---

## 📝 Key Technologies

| Component | Purpose | Version |
|-----------|---------|---------|
| **Apache Airflow** | Workflow orchestration | 3.1.8 |
| **dbt** | Data transformation framework | 1.7 |
| **DuckDB** | OLAP database engine | 1.5.1 |
| **PySpark** | Distributed processing | Latest |
| **Python** | Programming language | 3.11+ |
| **Docker** | Containerization | Latest |

---

## 📊 Dataset Information

**Source**: NYC Taxi & Limousine Commission  
**Period**: January - December 2023  
**Records**: ~38 million yellow taxi trips  
**Columns**: 19 original columns (pickup time, dropoff time, fare, tip, location IDs, etc.)  
**File Format**: Parquet  
**Total Size**: ~5-6GB

---

## 🔐 Security Notes

- `.env` file is in `.gitignore` — Never commit secrets
- Use `.env.example` as template for credentials
- Airflow default credentials should be changed in production
- Email alerts are disabled by default (no SMTP configured)

---

## 📋 GitHub Repository Files

See [GITHUB_FILES.md](GITHUB_FILES.md) for complete list of files to commit.

### Files to Include
✅ All Python, SQL, YAML, and Markdown files  
✅ `docker-compose.yaml`, `requirements.txt`  
✅ Configuration templates (`.env.example`)  

### Files to Exclude (in .gitignore)
❌ `data/` — Raw data files  
❌ `logs/` — Execution logs  
❌ `venv/` — Virtual environment  
❌ `.env` — Secrets  
❌ `*.duckdb` — Database files  
❌ `dbt/target/` — Compiled dbt output  

---

## 🤝 Contributing

1. Fork repository
2. Create feature branch (`git checkout -b feature/`)
3. Commit changes (`git commit -m 'Add feature'`)
4. Push to branch (`git push origin feature/`)
5. Open Pull Request

---

## 📄 License

This project is open source and available under the MIT License.

---

## 📚 Documentation

- [Airflow Setup Guide](AIRFLOW_SETUP.md)
- [Airflow Testing Guide](AIRFLOW_TESTING.md)
- [Blue/Green Deployment](BLUE_GREEN_DEPLOYMENT.md)
- [GitHub Files Reference](GITHUB_FILES.md)

---

## ✅ Verification Checklist

After setup, verify the pipeline works:

- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Data files downloaded and placed in `data/`
- [ ] Raw data loaded (`python load_data.py`)
- [ ] dbt models run successfully (`dbt run`)
- [ ] All tests pass (`dbt test`)
- [ ] Airflow services running (`docker-compose ps`)
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] DAG triggers successfully
- [ ] All tasks complete with status=success

---

## 📧 Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review Airflow logs: `docker-compose logs airflow-scheduler`
3. Check dbt logs: `dbt debug`
4. Open GitHub issue with error details

---

Happy data engineering! 🚀
│   ├── seeds/
│   │   └── taxi_zone_lookup.csv
│   └── dbt_project.yml
├── dags/
│   └── nyc_taxi_daily_pipeline.py # Airflow DAG orchestration
├── queries/
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   └── q3_consecutive_gap_analysis.sql
├── spark/
│   └── process_historical.py      # PySpark historical processing
├── load_data.py                   # Load raw data into DuckDB
├── requirements.txt
└── README.md
```

---

## DBT Models

### Staging Layer (Raw → Clean)
- **`stg_yellow_trips`** — Column renaming (snake_case), type casting, computed `trip_duration_minutes`
- **`stg_taxi_zones`** — Zone dimension from seed CSV

### Intermediate Layer (Transformation & Filtering)
- **`int_trips_enriched`** — Left-joins trips with zone dimensions (pickup/dropoff)
  - **Business Filters Applied:**
    - `trip_distance > 0` (no zero-distance trips)
    - `fare_amount > 0` (no free rides)
    - `passenger_count > 0` (minimum 1 passenger)
    - `trip_duration_minutes BETWEEN 1 AND 180` (1 min to 3 hours)

### Mart Layer (Analytics-Ready)
- **`fct_trips`** — Fact table with auto-generated `trip_id` primary key
  - All enriched, filtered trips with zone enrichment
  
- **`dim_zones`** — Zone dimension 
  - location_id (PK), borough, zone, service_zone
  
- **`agg_daily_revenue`** — Daily aggregations
  - trip_date (PK), total_trips, total_fare, avg_fare, total_tips, **tip_rate_percent**, total_revenue
  - Aggregated from `fct_trips` daily
  
- **`agg_zone_performance`** — Monthly zone performance metrics
  - pickup_zone, trip_month, total_trips, avg_trip_distance, avg_fare, total_revenue
  - **revenue_rank**: Ranked WITHIN each month (not globally)
    - Reveals seasonal trends and monthly top performers
    - More actionable for marketing/operations monthly strategy
  - **high_volume_zone**: Boolean flag for zones with > 10,000 trips/month

---

## Data Quality Tests

**Total: 64 Tests | 7 Model Coverage**

### Test Categories

| Category | Count | Examples |
|----------|-------|----------|
| Not Null | 30+ | All fact/dimension key columns |
| Unique | 8 | trip_id, location_id, trip_date |
| Relationships | 2 | Foreign keys: fct_trips → dim_zones |
| Custom Singular | 3 | total_amount ≥ fare, zone enrichment |
| Custom Generic | 1 | trip_duration_range (1-180 min) |

### Test Validation Strategy

- **Staging**: Minimal tests (data quality filters at intermediate layer)
- **Intermediate**: Business rule validations (filtering is applied here)
- **Fact/Marts**: Strict tests ensure downstream data quality
- **Test on Failure**: DAG fails if any test fails, preventing bad data exposure

---

## Airflow DAG: `nyc_taxi_daily_pipeline`

### DAG Configuration
```python
Schedule:     Daily at 02:00 UTC
Retries:      2 attempts with 5-minute delays
Email Alert:  On failure (configured via AIRFLOW_ALERT_EMAIL)
Backfill:     Enabled (can run for historical dates)
```

### Pipeline Tasks (Sequential)

1. **`check_source_freshness`** (PythonOperator)
   - Validates source data file exists for the execution date
   - Raises AirflowFailException if missing
   - Prevents pipeline execution without data

2. **`run_dbt_staging`** (BashOperator)
   - Builds staging models (stg_yellow_trips, stg_taxi_zones)
   - Command: `dbt run --select staging`

3. **`run_dbt_intermediate`** (BashOperator)
   - Builds intermediate enriched trips with zone joins
   - Command: `dbt run --select intermediate`
   - **Data filtering applied here** (invalid records removed)

4. **`run_dbt_marts`** (BashOperator)
   - Builds fact and aggregation tables
   - Command: `dbt run --select marts`

5. **`run_dbt_tests`** (BashOperator)
   - Runs all 64 data quality tests
   - Command: `dbt test`
   - **DAG fails if any test returns rows** (test failure)

6. **`notify_success`** (PythonOperator)
   - Logs success summary with daily metrics:
     - Total trips, revenue, avg fare
     - Top zones by performance
     - Data quality status
   - In production, sends email/Slack alert

### Task Dependencies (DAG Flow)
```
check_source_freshness 
    → run_dbt_staging 
    → run_dbt_intermediate 
    → run_dbt_marts 
    → run_dbt_tests 
    → notify_success
```

### Backfill Support
The DAG is designed for backfill scenarios:
- `catchup=True` allows running DAG for past dates
- Each task uses `execution_date` context to process specific day's data
- Suitable for reprocessing historical data

---

## Production Considerations: Blue/Green Deployment Strategy

### The Problem
If `run_dbt_tests` fails halfway through, downstream consumers might see partially updated, invalid data in the marts tables. This creates data consistency issues.

### Current Implementation
- **Single-environment** approach: Updates happen in-place
- **Risk**: Test failures expose bad data to consumers

### Recommended: Blue/Green Deployment Pattern

#### Architecture
```
Environment A (Blue)  - Current production tables
Environment B (Green) - Staging tables for new run

DAG Flow:
1. check_source_freshness
2. run_dbt_staging (to GREEN)
3. run_dbt_intermediate (to GREEN)
4. run_dbt_marts (to GREEN)
5. run_dbt_tests (against GREEN)
6. IF tests pass → SWAP (Green → Blue, old Blue → archive)
7. IF tests fail → DISCARD Green (keep Blue unchanged)
8. notify_success
```

#### Implementation Strategy

**Option 1: Naming Convention**
```sql
-- Current production
- fct_trips
- dim_zones
- agg_daily_revenue

-- New build (if tests pass, swap)
- fct_trips_new
- dim_zones_new
- agg_daily_revenue_new

-- After tests pass:
DROP TABLE fct_trips;
ALTER TABLE fct_trips_new RENAME TO fct_trips;
```

**Option 2: Schema-Based Separation**
```sql
-- Current production
prod.fct_trips
prod.dim_zones

-- New build
stage.fct_trips
stage.dim_zones

-- After tests pass:
BEGIN TRANSACTION;
  DROP SCHEMA prod;
  ALTER SCHEMA stage RENAME TO prod;
COMMIT;
```

**Option 3: DuckDB Attachment (Recommended for this stack)**
```sql
-- Separate DuckDB files
- prod_nyc_taxi.duckdb (current)
- stage_nyc_taxi.duckdb (new build)

-- DAG logic:
1-4. Build to stage_nyc_taxi.duckdb
5-6. Test stage_nyc_taxi.duckdb
7. If tests pass: rename prod → archive, stage → prod
8. If tests fail: delete stage, keep prod
```

#### DBT Configuration
```yaml
# profiles.yml - environment separation
nyc_taxi:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: ../data/prod_nyc_taxi.duckdb
    stage:
      type: duckdb
      path: ../data/stage_nyc_taxi.duckdb
```

#### DAG Implementation
```python
# In Airflow DAG:
run_dbt_marts = BashOperator(
    task_id='run_dbt_marts',
    bash_command='dbt run --profiles-dir ... --target stage'
)

def swap_environments():
    """Swap stage → prod if all tests passed"""
    import os
    import shutil
    os.rename('prod_nyc_taxi.duckdb', 'archive_nyc_taxi.duckdb')
    os.rename('stage_nyc_taxi.duckdb', 'prod_nyc_taxi.duckdb')

swap_task = PythonOperator(
    task_id='swap_environments',
    python_callable=swap_environments,
    trigger_rule='all_success'  # Only if tests pass
)

# Updated DAG flow:
... >> run_dbt_tests >> swap_task >> notify_success
```

#### Benefits
✅ **Data Consistency**: Bad data never reaches consumers  
✅ **Rollback**: Keep previous version for quick rollback  
✅ **Testing**: Full test suite against staged data  
✅ **Zero Downtime**: Atomic swap after validation  
✅ **Auditability**: Archive of all previous runs  

---

## Environment Variables Configuration

```bash
# Set in Airflow container or .env file
export DBT_DIR="/home/airflow/projects/nyc_taxi_full_project/dbt"
export DBT_PROFILES_DIR="~/.dbt"
export AIRFLOW_ALERT_EMAIL="data-team@company.com"
export DUCKDB_PATH="../data/nyc_taxi.duckdb"
```

---

## Analytical SQL Queries

### Q1: Top Zones by Revenue
```sql
SELECT pickup_zone, SUM(total_revenue) as revenue
FROM agg_zone_performance
GROUP BY pickup_zone
ORDER BY revenue DESC
LIMIT 10;
```

### Q2: Hour of Day Pattern
```sql
SELECT EXTRACT(HOUR FROM pickup_datetime) as hour,
       COUNT(*) as trip_count,
       AVG(fare_amount) as avg_fare
FROM fct_trips
GROUP BY 1
ORDER BY 1;
```

### Q3: Consecutive Gap Analysis
```sql
SELECT pickup_zone, DATE_TRUNC('month', pickup_datetime) as month,
       MAX(total_revenue) as max_revenue,
       RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) as rank
FROM agg_zone_performance
WHERE high_volume_zone = true
GROUP BY 1, 2;
```

---

## Known Issues & Fixes

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| dbt not recognized | Python not in PATH | Installed Python 3.11 + venv |
| PowerShell execution policy error | Windows security policy | `Set-ExecutionPolicy RemoteSigned` |
| profiles.yml missing | Manual setup needed | Created manually with DuckDB config |
| Seed CSV not found | Missing data | Downloaded `taxi_zone_lookup.csv` + `dbt seed` |
| Airflow fcntl error | Unix-only module on Windows | Use WSL2 or Docker for Airflow |
| NULL passenger_count (71k rows) | Source data quality | Filtered at intermediate layer |
| Invalid fare/total amounts (25k rows) | Source data quality | Filtered at intermediate layer |

---

## Performance Optimization

### DBT Incremental Models
For production with daily runs, consider incremental models:
```sql
{{ config(materialized='incremental') }}

SELECT ... FROM raw_yellow_trips
{% if execute %}
  WHERE pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}
```

### Parallelization in Airflow
```python
# Process zones in parallel after marts ready
zone_tasks = [
  BashOperator(f'dbt run --select tag:zone_{i}')
  for i in range(10)
]
```

---

## Conclusion

This project demonstrates:
- ✅ **Data Modelling**: Clean DBT transformation layers with proper testing
- ✅ **Data Quality**: 64 comprehensive tests across all model layers
- ✅ **Orchestration**: Production-ready Airflow DAG with error handling
- ✅ **Scalability**: PySpark support for historical processing
- ✅ **Best Practices**: Blue/green deployment, environment separation, backfill support

For production deployment, implement the blue/green strategy to ensure data consistency and enable safe rollbacks.
