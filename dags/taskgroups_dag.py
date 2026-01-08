from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import random


# Simple functions for the operators
def extract_data(source):
    """Extract data from a source."""
    print(f"Extracting data from {source}")
    # Simulate random failures
    if random.random() > 0.7:
        raise Exception(f"Failed to extract from {source}")
    print(f"✓ Successfully extracted from {source}")
    return f"data_from_{source}"


def transform_data(data):
    """Transform the extracted data."""
    print(f"Transforming {data}")
    print("✓ Transformation complete")
    return f"transformed_{data}"


def load_data(data):
    """Load data to destination."""
    print(f"Loading {data}")
    print("✓ Data loaded successfully")


def send_success_alert():
    """Send success notification."""
    print("📧 SUCCESS: Pipeline completed successfully!")


def send_failure_alert():
    """Send failure notification."""
    print("⚠️ FAILURE: Pipeline encountered errors!")


def cleanup_resources():
    """Cleanup regardless of success or failure."""
    print("🧹 Cleaning up temporary resources...")
    print("✓ Cleanup complete")


# Define the DAG
with DAG(
    dag_id='trigger_rules_taskgroups_pools',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': None,
        'retry_delay': timedelta(minutes=2),
    },
    tags=['example', 'trigger-rules', 'task-groups', 'pools'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ETL pipeline..."',
    )

    # ============================================
    # TASK GROUP 1: Data Extraction (with POOLS)
    # ============================================
    # Task groups organize related tasks visually
    
    with TaskGroup('extraction_group', tooltip='Extract data from multiple sources') as extraction_group:
        # POOL: 'database_pool' - limits concurrent database connections
        # Use pool='database_pool' to assign tasks to a pool
        
        extract_mysql = PythonOperator(
            task_id='extract_from_mysql',
            python_callable=extract_data,
            op_kwargs={'source': 'mysql'},
            pool='database_pool',  # ← POOL 1: Limits concurrent DB access
        )
        
        extract_postgres = PythonOperator(
            task_id='extract_from_postgres',
            python_callable=extract_data,
            op_kwargs={'source': 'postgres'},
            pool='database_pool',  # ← POOL 1: Same pool as above
        )
        
        # POOL: 'api_pool' - limits concurrent API calls
        extract_api = PythonOperator(
            task_id='extract_from_api',
            python_callable=extract_data,
            op_kwargs={'source': 'api'},
            pool='api_pool',  # ← POOL 2: Different pool for APIs
        )

    # ============================================
    # TASK GROUP 2: Data Transformation
    # ============================================
    
    with TaskGroup('transformation_group', tooltip='Transform extracted data') as transformation_group:
        transform_mysql = PythonOperator(
            task_id='transform_mysql_data',
            python_callable=transform_data,
            op_kwargs={'data': 'mysql_data'},
        )
        
        transform_postgres = PythonOperator(
            task_id='transform_postgres_data',
            python_callable=transform_data,
            op_kwargs={'data': 'postgres_data'},
        )
        
        transform_api = PythonOperator(
            task_id='transform_api_data',
            python_callable=transform_data,
            op_kwargs={'data': 'api_data'},
        )

    # ============================================
    # Data Loading Task
    # ============================================
    
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_data,
        op_kwargs={'data': 'all_transformed_data'},
        pool='database_pool',  # ← Uses the database pool again
    )

    # ============================================
    # TRIGGER RULE 1: ONE_SUCCESS
    # ============================================
    # Runs if AT LEAST ONE upstream task succeeds
    # Use case: Send success alert if ANY extraction worked
    
    success_alert = PythonOperator(
        task_id='send_success_alert',
        python_callable=send_success_alert,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # ← TRIGGER RULE 1
    )

    # ============================================
    # TRIGGER RULE 2: ALL_FAILED
    # ============================================
    # Runs ONLY if ALL upstream tasks fail
    # Use case: Send failure alert only if everything failed
    
    failure_alert = PythonOperator(
        task_id='send_failure_alert',
        python_callable=send_failure_alert,
        trigger_rule=TriggerRule.ALL_FAILED,  # ← TRIGGER RULE 2
    )

    # ============================================
    # TRIGGER RULE 3 (BONUS): ALL_DONE
    # ============================================
    # Runs when ALL upstream tasks are done (success OR fail)
    # Use case: Always cleanup resources regardless of outcome
    
    cleanup = PythonOperator(
        task_id='cleanup_resources',
        python_callable=cleanup_resources,
        trigger_rule=TriggerRule.ALL_DONE,  # ← BONUS: Always runs
    )

    # ============================================
    # TASK DEPENDENCIES
    # ============================================
    
    # Start → Extract (task group)
    start >> extraction_group
    
    # Extract → Transform (task group) - individual task dependencies
    extraction_group >> transformation_group
    
    # Transform → Load
    transformation_group >> load
    
    # Load → Alerts (both success and failure are triggered from load)
    load >> [success_alert, failure_alert]
    
    # Both alerts → Cleanup (cleanup runs after both alerts are done)
    [success_alert, failure_alert] >> cleanup