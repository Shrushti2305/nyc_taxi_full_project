# Blue/Green Deployment Strategy for NYC Taxi Pipeline

## Executive Summary

This document describes how to implement a blue/green deployment pattern to ensure data consistency and enable safe rollbacks for the NYC Taxi Mart Layer.

---

## Problem Statement

### Current Risk
In the current single-environment implementation:
- If `run_dbt_tests` fails during a pipeline run, partially updated data is already visible to downstream consumers
- Inconsistent state: Some tables updated, others not
- No ability to safely rollback

### Consequences
- ❌ Data consumers see contradictory metrics
- ❌ Historical data integrity compromised
- ❌ Complex debugging and recovery procedures
- ❌ Loss of trust in analytics layer

---

## Blue/Green Pattern Overview

### Architecture
```
PRODUCTION (Blue)              STAGING (Green)
├── prod.fct_trips             ├── stage.fct_trips
├── prod.dim_zones             ├── stage.dim_zones
├── prod.agg_daily_revenue     ├── stage.agg_daily_revenue
└── prod.agg_zone_performance  └── stage.agg_zone_performance
        ↑                             ↑
   [In Use]                    [New Run]
   
   After successful tests:
   swap(Blue ← Green)
```

### Key Benefits
✅ **Atomicity**: All-or-nothing update to production  
✅ **Rollback**: Keep previous version for instant rollback  
✅ **Testing**: Full test suite against staged data before exposure  
✅ **Zero Downtime**: Queries never interrupted  
✅ **Auditability**: Archive all versions  

---

## Implementation Approaches

### Approach 1: Separate DuckDB Files (Recommended for this project)

**Rationale**: Simple, atomic file-level operations

#### Setup
```bash
data/
├── prod_nyc_taxi.duckdb        # Current production
├── stage_nyc_taxi.duckdb       # New build (temporary)
└── archive_nyc_taxi.duckdb     # Previous version (for rollback)
```

#### DBT Configuration (profiles.yml)
```yaml
nyc_taxi:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: ../data/prod_nyc_taxi.duckdb
      threads: 4
    
    stage:
      type: duckdb
      path: ../data/stage_nyc_taxi.duckdb
      threads: 4
```

#### Airflow DAG Flow
```python
# Step 1-4: Build to staging database
run_dbt_staging = BashOperator(
    task_id='run_dbt_staging',
    bash_command='dbt run --select staging --target stage',
)

run_dbt_intermediate = BashOperator(
    task_id='run_dbt_intermediate',
    bash_command='dbt run --select intermediate --target stage',
)

run_dbt_marts = BashOperator(
    task_id='run_dbt_marts',
    bash_command='dbt run --select marts --target stage',
)

# Step 5: Test the staged database
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='dbt test --target stage',
)

# Step 6: If tests pass, swap environments
def swap_environments():
    """Atomically swap stage → prod"""
    import os
    import logging
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    
    prod_path = '../data/prod_nyc_taxi.duckdb'
    stage_path = '../data/stage_nyc_taxi.duckdb'
    archive_dir = '../data/archive'
    
    os.makedirs(archive_dir, exist_ok=True)
    
    # Archive current production
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_path = f'{archive_dir}/prod_nyc_taxi_{timestamp}.duckdb'
    
    logger.info(f"Swapping databases...")
    logger.info(f"  Archiving: {prod_path} → {archive_path}")
    os.rename(prod_path, archive_path)
    
    logger.info(f"  Promoting: {stage_path} → {prod_path}")
    os.rename(stage_path, prod_path)
    
    logger.info(f"✅ Swap complete. Archive available at: {archive_path}")
    return archive_path

swap_task = PythonOperator(
    task_id='swap_environments',
    python_callable=swap_environments,
    trigger_rule='all_success',  # Only run if all upstream tasks succeed
)

def rollback_on_failure():
    """Cleanup staging database on test failure"""
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    stage_path = '../data/stage_nyc_taxi.duckdb'
    
    if os.path.exists(stage_path):
        logger.warning(f"Tests failed. Removing staging database...")
        os.remove(stage_path)
        logger.info(f"✅ Cleanup complete. Production database unchanged.")

cleanup_task = PythonOperator(
    task_id='cleanup_staging',
    python_callable=rollback_on_failure,
    trigger_rule='one_failed',  # Only run if upstream tasks failed
)

# DAG Dependencies
check_source_freshness >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests
run_dbt_tests >> swap_task >> notify_success
run_dbt_tests >> cleanup_task
```

---

### Approach 2: Schema-Based Separation

**Rationale**: If using PostgreSQL or similar with schema support

#### Setup
```sql
CREATE SCHEMA prod;
CREATE SCHEMA stage;
CREATE SCHEMA archive;

-- Initial tables in prod schema
CREATE TABLE prod.fct_trips AS SELECT ...;
```

#### DBT Profiles
```yaml
nyc_taxi:
  outputs:
    prod:
      type: postgres
      schema: prod
    stage:
      type: postgres
      schema: stage
```

#### Atomic Swap (Transaction)
```python
def swap_schemas():
    """Atomically swap production and staging schemas"""
    import psycopg2
    
    conn = psycopg2.connect("dbname=analytics user=airflow")
    cursor = conn.cursor()
    
    try:
        cursor.execute("BEGIN;")
        
        # Rename schemas atomically
        cursor.execute("ALTER SCHEMA prod RENAME TO archive;")
        cursor.execute("ALTER SCHEMA stage RENAME TO prod;")
        
        cursor.execute("COMMIT;")
        logger.info("✅ Schema swap complete (transactional)")
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        logger.error(f"❌ Swap failed, rolled back: {e}")
        raise

swap_task = PythonOperator(
    task_id='swap_schemas',
    python_callable=swap_schemas,
    trigger_rule='all_success',
)
```

---

### Approach 3: View-Based Redirection (Most Flexible)

**Rationale**: Zero-downtime without file swaps

#### Setup
```sql
-- Physical tables
CREATE TABLE fct_trips_v1 AS SELECT ...;
CREATE TABLE fct_trips_v2 AS SELECT ...;

-- View for consumers (points to production)
CREATE MATERIALIZED VIEW fct_trips AS SELECT * FROM fct_trips_v1;
```

#### Swap Logic
```python
def swap_views():
    """Swap materialized view to point to new version"""
    import duckdb
    
    conn = duckdb.connect('../data/nyc_taxi.duckdb')
    
    # Create new version
    conn.execute("""
        CREATE TABLE fct_trips_v2 AS
        SELECT * FROM (query for new data)
    """)
    
    # Atomic swap
    conn.execute("DROP MATERIALIZED VIEW fct_trips;")
    conn.execute("""
        CREATE MATERIALIZED VIEW fct_trips AS
        SELECT * FROM fct_trips_v2;
    """)
    
    logger.info("✅ View swapped to fct_trips_v2")
```

---

## Error Handling & Rollback

### Automatic Rollback on Test Failure
```python
# If tests fail, staging database is cleaned up
cleanup_task = PythonOperator(
    task_id='cleanup_on_failure',
    python_callable=cleanup_staging,
    trigger_rule='one_failed',
)

# Production remains untouched
```

### Manual Rollback Procedure
```bash
# If issues detected after swap:

# 1. Identify archive version
ls -lh data/archive/

# 2. Manual rollback
mv data/prod_nyc_taxi.duckdb data/current_issue_nyc_taxi.duckdb
mv data/archive/prod_nyc_taxi_20240125_140000.duckdb data/prod_nyc_taxi.duckdb

# 3. Notify consumers
# Send alert that analytics data has been reverted to previous version
```

---

## Monitoring & Validation

### Pre-Swap Validation
```python
def validate_before_swap():
    """Verify data quality before promoting to production"""
    import duckdb
    
    stage_conn = duckdb.connect('../data/stage_nyc_taxi.duckdb')
    prod_conn = duckdb.connect('../data/prod_nyc_taxi.duckdb')
    
    # Compare row counts
    stage_rows = stage_conn.execute(
        "SELECT COUNT(*) FROM fct_trips"
    ).fetchall()[0][0]
    prod_rows = prod_conn.execute(
        "SELECT COUNT(*) FROM fct_trips"
    ).fetchall()[0][0]
    
    logger.info(f"Stage: {stage_rows} rows vs Prod: {prod_rows} rows")
    
    # Fail if too much variance (unexpected data loss)
    if stage_rows < prod_rows * 0.9:
        raise Exception("Stage has 10%+ fewer rows than production!")
    
    # Check key metrics haven't changed drastically
    stage_revenue = stage_conn.execute(
        "SELECT SUM(total_revenue) FROM agg_daily_revenue"
    ).fetchall()[0][0]
    
    logger.info(f"Stage total revenue: ${stage_revenue:,.2f}")
    return True
```

### Post-Swap Validation
```python
def validate_after_swap():
    """Verify production is working correctly"""
    import duckdb
    
    prod_conn = duckdb.connect('../data/prod_nyc_taxi.duckdb')
    
    # Run quick smoke tests
    queries = {
        'fct_trips': "SELECT COUNT(*) FROM fct_trips",
        'agg_daily_revenue': "SELECT COUNT(*) FROM agg_daily_revenue",
        'zone_performance': "SELECT COUNT(*) FROM agg_zone_performance",
    }
    
    results = {}
    for name, query in queries.items():
        count = prod_conn.execute(query).fetchall()[0][0]
        results[name] = count
        logger.info(f"✅ {name}: {count:,} rows")
    
    return results
```

---

## Operational Procedures

### Daily Deployment Checklist
- [ ] Source data file verified for the day
- [ ] Staging build completes
- [ ] All 64 tests pass against staging
- [ ] Pre-swap validation succeeds
- [ ] Swap completes atomically
- [ ] Post-swap validation confirms production is healthy
- [ ] Success notification sent with metrics
- [ ] Archive file verified in backup location

### Incident Response
1. **Issue Detected**: Rollback to previous version
   ```bash
   mv prod_nyc_taxi.duckdb prod_nyc_taxi_issue.duckdb
   mv archive/prod_nyc_taxi_TIMESTAMP.duckdb prod_nyc_taxi.duckdb
   ```

2. **Notify Consumers**: Send alert that data reverted

3. **Investigate**: Compare issue database vs current
   ```sql
   duckdb prod_nyc_taxi_issue.duckdb
   SELECT COUNT(*), SUM(total_revenue) FROM fct_trips;
   ```

4. **Fix DBT Models**: Update code that caused issue

5. **Rerun Pipeline**: Execute DAG again

---

## Cost & Performance Considerations

| Approach | Disk Usage | Swap Time | Complexity |
|----------|-----------|-----------|-----------|
| File Swap | 2x DB size | < 1 sec | Low |
| Schema Swap | 2x data | Instant (transaction) | Medium |
| View Redirect | 2x data | Instant | Medium |

### Recommendation
**Use File Swap (Approach 1) for this project** because:
- Simplest to implement
- DuckDB files are atomic
- Easy to version and archive
- No schema management overhead
- Natural disaster recovery (keep archives on different storage)

---

## Implementation Timeline

### Phase 1 (Immediate)
- [ ] Add `stage` target to `profiles.yml`
- [ ] Create `swap_environments.py` module
- [ ] Update DAG with swap task
- [ ] Test on historical data

### Phase 2 (Next Sprint)
- [ ] Implement pre/post-swap validation
- [ ] Add automated rollback on test failure
- [ ] Document manual rollback procedure
- [ ] Train team on procedures

### Phase 3 (Production)
- [ ] Deploy with blue/green active
- [ ] Monitor swap performance
- [ ] Archive versioning strategy
- [ ] Disaster recovery testing

---

## References
- [Blue/Green Deployment Pattern](https://martinfowler.com/bliki/BlueGreenDeployment.html)
- [Zero-Downtime Deployments](https://www.atlassian.com/continuous-delivery/principles/blue-green-deployments)
- [DuckDB Documentation](https://duckdb.org/docs/)
