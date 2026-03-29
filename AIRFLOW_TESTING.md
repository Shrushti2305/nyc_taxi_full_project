# Airflow Testing & Debugging Guide

## Quick Test Procedures

### 1. Validate DAG Syntax (No Setup Required)
```bash
cd <project_root>

# Check if DAG file has syntax errors
python -m py_compile dags\nyc_taxi_daily_pipeline.py

# Should output nothing if successful
```

---

## Setup Airflow Locally (First Time Only)

### Step 1: Set Environment Variables (PowerShell)
**Important:** Use `$env:` prefix in PowerShell

```powershell
# Set environment variables for this session
$env:AIRFLOW_HOME = "<project_root>\airflow"
$env:DBT_DIR = "<project_root>\dbt"
$env:DBT_PROFILES_DIR = "C:\Users\<username>\.dbt"
$env:AIRFLOW_ALERT_EMAIL = "your-email@example.com"

# Verify they're set
$env:AIRFLOW_HOME
$env:DBT_DIR
```

**Optional:** Persist for future sessions
```powershell
[Environment]::SetEnvironmentVariable('AIRFLOW_HOME', $env:AIRFLOW_HOME, 'User')
[Environment]::SetEnvironmentVariable('DBT_DIR', $env:DBT_DIR, 'User')
[Environment]::SetEnvironmentVariable('DBT_PROFILES_DIR', $env:DBT_PROFILES_DIR, 'User')
[Environment]::SetEnvironmentVariable('AIRFLOW_ALERT_EMAIL', $env:AIRFLOW_ALERT_EMAIL, 'User')
```

### Step 2: Initialize Airflow Database
```bash
cd <project_root>

airflow db init
# Creates airflow/airflow.db SQLite database
# Takes ~30 seconds
```

### Step 3: Create Admin User
```bash
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com `
  --password airflow123
```

### Step 4: Verify DAG is Recognized
```bash
# List all DAGs
airflow dags list

# Should show: nyc_taxi_daily_pipeline

# Get detailed DAG info
airflow dags info --dag-id nyc_taxi_daily_pipeline
```

---

## Test Individual Tasks (Without Webserver)

### Test Single Task Execution
```bash
# Run one task for a specific date (doesn't require scheduler)
airflow tasks test nyc_taxi_daily_pipeline check_source_freshness 2023-01-01

# Output shows:
# - Task execution
# - Any print() statements or logs
# - Success/failure status
# - Execution time
```

### Test Task with Debug Logging
```bash
# Run with verbose output
airflow tasks test --loglevel DEBUG nyc_taxi_daily_pipeline run_dbt_staging 2023-01-01
```

### Test All Tasks Sequentially (Dry Run)
```bash
# Runs full DAG without scheduler
# Good for development testing
airflow dags test nyc_taxi_daily_pipeline 2023-01-01

# Output:
# [check_source_freshness] → [run_dbt_staging] → ... → [notify_success]
```

---

## Start Full Airflow Application (With Logs)

### Terminal 1: Start Webserver
```bash
cd <project_root>

# Start webserver on port 8080
airflow webserver --port 8080

# Output shows:
# [2024-01-25 14:30:15] Starting Airflow webserver with...
# [2024-01-25 14:30:20] Running on http://127.0.0.1:8080

# Open browser: http://localhost:8080
# Login: admin / airflow123
```

### Terminal 2: Start Scheduler
```bash
cd <project_root>

# Start scheduler (in separate terminal/tab)
airflow scheduler

# Output shows:
# [2024-01-25 14:30:30] Starting Airflow scheduler
# [2024-01-25 14:30:32] [SchedulerJob] Server started
# [2024-01-25 14:30:35] [SchedulerJob] Spawned 1 job(s)
```

### Terminal 3: Monitor Logs (Optional)
```bash
# Monitor all Airflow logs in real-time
cd $env:AIRFLOW_HOME\logs
Get-Content -Path "dag_id=nyc_taxi_daily_pipeline/*" -Wait

# Or tail logs for specific task
Get-Content -Path "dag_id=nyc_taxi_daily_pipeline/run_dbt_staging*" -Wait
```

---

## Trigger DAG and Check Logs

### Via Webserver (Easiest)
1. Open http://localhost:8080
2. Login: admin / airflow123
3. Find "nyc_taxi_daily_pipeline" DAG
4. Click "Trigger DAG" button
5. Observe execution in DAG graph view
6. Click on each task to view logs

### Via Command Line (Airflow 3.1.8)
```bash
# Trigger DAG
airflow dags trigger nyc_taxi_daily_pipeline

# View execution status
airflow dags list-runs --dag-id nyc_taxi_daily_pipeline

# Get task logs (most recent run)
airflow tasks logs nyc_taxi_daily_pipeline run_dbt_staging

# Get logs for specific execution date
airflow tasks logs nyc_taxi_daily_pipeline run_dbt_staging --execution-date 2023-01-01T00:00:00+00:00

# Watch logs in real-time
Get-Content -Path "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\run_dbt_staging\*\*.log" -Wait
```

---

## Log File Locations

### Webserver Logs
```
airflow/logs/webserver.log
```

### Scheduler Logs
```
airflow/logs/scheduler.log
```

### Task Logs (Detailed)
```
airflow/logs/dag_id=nyc_taxi_daily_pipeline/
├── check_source_freshness/
│   └── 2023-01-01T02:00:00+00:00/
│       └── attempt-1.log
├── run_dbt_staging/
│   └── 2023-01-01T02:00:00+00:00/
│       └── attempt-1.log
└── run_dbt_tests/
    └── 2023-01-01T02:00:00+00:00/
        └── attempt-1.log
```

### View Task Logs
```bash
# Show first 100 lines
Get-Content "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\run_dbt_staging\*\*.log" -Head 100

# Show last 50 lines (most recent)
Get-Content "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\run_dbt_staging\*\*.log" -Tail 50

# Search for errors
Select-String "ERROR" "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\run_dbt_staging\*\*.log"
```

---

## Testing Workflow

### For Development (Quick Iteration)
```bash
# 1. Make changes to DAG
# 2. Check syntax
python -m py_compile dags\nyc_taxi_daily_pipeline.py

# 3. Test single modified task
airflow tasks test nyc_taxi_daily_pipeline check_source_freshness 2023-01-01

# 4. View task logs
Get-Content -Path "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\check_source_freshness\*\*.log" -Tail 50

# 5. If successful, test full DAG
airflow dags test nyc_taxi_daily_pipeline 2023-01-01

# 6. Check final logs
Get-Content -Path "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\notify_success\*\*.log"
```

### For Full Integration (With Scheduler)
```bash
# 1. Start webserver (Terminal 1)
airflow webserver --port 8080

# 2. Start scheduler (Terminal 2)
airflow scheduler

# 3. Trigger DAG via CLI or webserver
airflow dags trigger --dag-id nyc_taxi_daily_pipeline --exec-date 2023-01-01

# 4. Monitor via webserver UI
# http://localhost:8080 → DAG Graph View

# 5. View task logs
airflow tasks logs nyc_taxi_daily_pipeline run_dbt_tests 2023-01-01T02:00:00+00:00
```

---

## Common Testing Scenarios

### Scenario 1: Test Only DBT Staging
```bash
# Single task test
airflow tasks test nyc_taxi_daily_pipeline run_dbt_staging 2023-01-01

# Check if dbt runs successfully
# Output will show: [INFO] dbt invocation: dbt run --select staging ...
```

### Scenario 2: Test DBT Tests Specifically
```bash
# Run test task only
airflow tasks test nyc_taxi_daily_pipeline run_dbt_tests 2023-01-01

# Check if all 64 tests pass
# Output will show test results
```

### Scenario 3: Test Backfill (Multiple Dates)
```bash
# Backfill 5 days
airflow dags backfill \
  --dag-id nyc_taxi_daily_pipeline \
  --start-date 2023-01-01 \
  --end-date 2023-01-05 \
  --clear  # Reset any existing runs

# View all runs
airflow dags list-runs --dag-id nyc_taxi_daily_pipeline

# Check individual day logs
airflow tasks logs nyc_taxi_daily_pipeline run_dbt_marts 2023-01-03T02:00:00+00:00
```

### Scenario 4: Simulate Task Failure
```bash
# Manually mark task as failed to test retry logic
airflow tasks failed --dag-id nyc_taxi_daily_pipeline --task-id run_dbt_tests --execution-date 2023-01-01

# Then trigger again to test retry
airflow dags trigger --dag-id nyc_taxi_daily_pipeline --exec-date 2023-01-02

# Check if task retries (should happen 2x with 5-min delay)
```

---

## Debugging Tips

### Show DAG Dependencies
```bash
airflow dags show --dag-id nyc_taxi_daily_pipeline
```

### Print DAG Structure
```bash
airflow dags info --dag-id nyc_taxi_daily_pipeline
```

### List All Tasks in DAG
```bash
airflow tasks list --dag-id nyc_taxi_daily_pipeline

# Output:
# check_source_freshness
# run_dbt_staging
# run_dbt_intermediate
# run_dbt_marts
# run_dbt_tests
# notify_success
```

### Check Latest DAG Run Status
```bash
airflow dags state --dag-id nyc_taxi_daily_pipeline 2023-01-01T02:00:00+00:00

# Output: success, failed, running, etc.
```

### View Task State
```bash
airflow tasks state --dag-id nyc_taxi_daily_pipeline --task-id run_dbt_tests --execution-date 2023-01-01T02:00:00+00:00
```

---

## Log Analysis Examples

### Find Errors in Logs
```bash
$logs = "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline"

# Find all errors across all tasks
Get-ChildItem -Path $logs -Recurse -Filter "*.log" | 
  Select-String -Pattern "ERROR|FAILED|Exception" | 
  Select-Object Path, Line

# Show context around errors (3 lines before/after)
Get-ChildItem -Path $logs -Recurse -Filter "*.log" | 
  Select-String -Pattern "ERROR" -Context 3,3
```

### Monitor Task Duration
```bash
# Extract timing from logs
$logfile = "$env:AIRFLOW_HOME\logs\dag_id=nyc_taxi_daily_pipeline\run_dbt_marts\*\*.log"

Get-Content $logfile | 
  Select-String "Task duration|completed"
```

### Check Memory/CPU Usage
```bash
# During task execution, monitor system resources
Get-Process airflow* | Select-Object Name, CPU, Memory
```

---

## Webserver UI Navigation

1. **Home Page**: Overview of all DAGs
2. **DAG List**: Shows all DAGs with status
3. **DAG Detail**: Click DAG name → See task dependencies
4. **Tree View**: Time-based view of DAG runs
5. **Graph View**: Dependency graph (best for monitoring)
6. **Task Logs**: Click task instance → View logs
7. **Run Status**: Success/Failed indicators
8. **Admin Panel**: Connections, variables, users

### Quick Links
- http://localhost:8080/home — DAG overview
- http://localhost:8080/dags/nyc_taxi_daily_pipeline/graph — DAG graph
- http://localhost:8080/admin/connection/ — Manage connections
- http://localhost:8080/admin/variable/ — Manage variables

---

## Troubleshooting

### DAG Not Appearing
```bash
# Check DAG syntax
python -m py_compile dags\nyc_taxi_daily_pipeline.py

# Restart scheduler (scheduler caches DAGs)
# Stop scheduler, wait 5s, restart

# Check AIRFLOW_HOME is set
$env:AIRFLOW_HOME
```

### Task Fails Immediately
```bash
# Check logs for actual error
airflow tasks logs --dag-id nyc_taxi_daily_pipeline --task-id run_dbt_staging --execution-date 2023-01-01T02:00:00+00:00

# Common: dbt not in PATH
which dbt
$env:PATH  # Check if DBT_DIR is included
```

### Logs Not Updating
```bash
# Logs update in real-time during execution
# Wait for task to complete

# Or manually refresh logs
Get-Content -Path "$env:AIRFLOW_HOME\logs\dag_id=*\*\*\*.log" -Wait
```

---

## Quick Commands Reference

```bash
# Syntax validation
python -m py_compile dags\nyc_taxi_daily_pipeline.py

# List DAGs
airflow dags list

# Test single task
airflow tasks test nyc_taxi_daily_pipeline {task_id} 2023-01-01

# Test full DAG
airflow dags test nyc_taxi_daily_pipeline 2023-01-01

# Trigger DAG
airflow dags trigger --dag-id nyc_taxi_daily_pipeline --exec-date 2023-01-01

# View logs
airflow tasks logs --dag-id nyc_taxi_daily_pipeline --task-id {task_id} --execution-date 2023-01-01T02:00:00+00:00

# Start webserver
airflow webserver --port 8080

# Start scheduler
airflow scheduler

# Backfill
airflow dags backfill --dag-id nyc_taxi_daily_pipeline --start-date 2023-01-01 --end-date 2023-01-05
```

---

## Next Steps

1. ✅ Test DAG syntax: `python -m py_compile dags\nyc_taxi_daily_pipeline.py`
2. ✅ Initialize Airflow: `airflow db init`
3. ✅ Create user: `airflow users create ...`
4. ✅ Test single task: `airflow tasks test ... 2023-01-01`
5. ✅ Start application: `airflow webserver` + `airflow scheduler`
6. ✅ Monitor via http://localhost:8080
