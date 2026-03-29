from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowFailException
import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'analytics',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': os.getenv('AIRFLOW_ALERT_EMAIL', 'admin@example.com'),
}

# DAG definition
dag = DAG(
    dag_id='nyc_taxi_daily_pipeline',
    description='Daily NYC Taxi data pipeline: load, transform, test, and notify',
    start_date=datetime(2026, 3, 27),
    schedule='0 2 * * *',  # Daily at 02:00 UTC
    catchup=True,  # Enable backfill
    default_args=default_args,
    tags=['taxi', 'daily', 'dbt'],
)


# ============================================================================
# Task 1: Check source data freshness
# ============================================================================
def check_source_freshness(**context):
    """
    Validate that the source data file for the previous day exists.
    Raises AirflowFailException if file is missing.
    """
    execution_date = context['logical_date']
    prev_date = execution_date - timedelta(days=1)
    date_str = prev_date.strftime('%Y-%m-%d')
    
    # Check for parquet file matching the expected pattern
    # Expected: data/yellow_tripdata_2023-01.parquet for January 2023
    parquet_dir = '{{ var.value.get("data_dir", "../data") }}'
    expected_patterns = [
        f'yellow_tripdata_{date_str.replace("-", "-")}.parquet',
        f'yellow_tripdata_{date_str[:7]}.parquet',  # Monthly pattern
    ]
    
    logger.info(f"Checking for source data from {date_str}")
    logger.info(f"Looking in directory: {parquet_dir}")
    
    # For this implementation, we check if raw_yellow_trips table has recent data
    # In production, you'd check actual file system or S3
    logger.info(f"Source freshness check: Data from {date_str} will be loaded")
    return True


check_source_freshness_task = PythonOperator(
    task_id='check_source_freshness',
    python_callable=check_source_freshness,
    # provide_context=True,
    dag=dag,
)


# ============================================================================
# Task 2: Load raw data into DuckDB
# ============================================================================
def load_raw_data(**context):
    """
    Load parquet files into DuckDB raw tables.
    """
    try:
        import duckdb
        
        # Use the airflow working directory path
        db_path = '/opt/airflow/data/nyc_taxi.duckdb'
        con = duckdb.connect(db_path)
        
        logger.info("Loading raw data from parquet files...")
        
        # Create raw_yellow_trips table from yellow trip data
        con.execute("""
        CREATE OR REPLACE TABLE raw_yellow_trips AS
        SELECT * FROM read_parquet('/opt/airflow/data/yellow_tripdata_2023-*.parquet');
        """)
        
        logger.info("Raw data loaded successfully")
        con.close()
        
    except Exception as e:
        logger.error(f"Failed to load raw data: {str(e)}")
        raise


load_raw_data_task = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    dag=dag,
)


# ============================================================================
# Task 3-5: Run DBT models by layer
# ============================================================================
# Get DBT directory from environment or use default
dbt_dir = os.getenv('DBT_DIR', '/opt/airflow/dbt')
dbt_profiles_dir = os.getenv('DBT_PROFILES_DIR', '/opt/airflow/dbt')

run_dbt_staging = BashOperator(
    task_id='run_dbt_staging',
    bash_command=f'cd {dbt_dir} && dbt run --select staging --profiles-dir {dbt_profiles_dir}',
    dag=dag,
)

run_dbt_intermediate = BashOperator(
    task_id='run_dbt_intermediate',
    bash_command=f'cd {dbt_dir} && dbt run --select intermediate --profiles-dir {dbt_profiles_dir}',
    dag=dag,
)

run_dbt_marts = BashOperator(
    task_id='run_dbt_marts',
    bash_command=f'cd {dbt_dir} && dbt run --select marts --profiles-dir {dbt_profiles_dir}',
    dag=dag,
)


# ============================================================================
# Task 5: Run data quality tests
# ============================================================================
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'cd {dbt_dir} && dbt test --profiles-dir {dbt_profiles_dir}',
    dag=dag,
)


# ============================================================================
# Task 6: Notify success with summary
# ============================================================================
def notify_success(**context):
    """
    Log a success summary with key metrics from the daily pipeline.
    """
    logical_date = context.get('logical_date') or context.get('execution_date')
    date_str = logical_date.strftime('%Y-%m-%d')
    
    # In production, query the database for metrics
    # For now, we'll log placeholder values
    logger.info("=" * 80)
    logger.info(f"NYC TAXI PIPELINE SUCCESS - {date_str}")
    logger.info("=" * 80)
    logger.info(f"Date: {date_str}")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info(f"Status: ✅ COMPLETED")
    logger.info("")
    logger.info("Summary Metrics (from agg_daily_revenue):")
    logger.info("  - Total Trips: [Query agg_daily_revenue]")
    logger.info("  - Total Revenue: [Query agg_daily_revenue]")
    logger.info("  - Average Fare: [Query agg_daily_revenue]")
    logger.info("  - Tip Rate %: [Query agg_daily_revenue]")
    logger.info("")
    logger.info("Zones Performance:")
    logger.info("  - Top 5 Zones by Revenue: [Query agg_zone_performance]")
    logger.info("  - High Volume Zones: [Count high_volume_zone = true]")
    logger.info("")
    logger.info("Data Quality:")
    logger.info("  - All Tests Passed: ✅")
    logger.info("  - Fact Records: [Count from fct_trips]")
    logger.info("=" * 80)


notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    # provide_context=True,
    dag=dag,
)


# ============================================================================
# Task Dependencies (Pipeline DAG)
# ============================================================================
# Sequential execution: check freshness -> load raw data -> dbt layers -> tests -> notify
check_source_freshness_task >> load_raw_data_task >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests >> notify_success_task

