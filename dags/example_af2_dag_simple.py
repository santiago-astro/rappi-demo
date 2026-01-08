from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Python functions for the operators
def process_file(file_name, base_path, date):
    """Process a single file with given parameters."""
    print(f"Processing file: {file_name}")
    print(f"Path: {base_path}")
    print(f"date: {date}")
    print("-" * 50)
    return f"{file_name} processed successfully"

# Define the DAG
with DAG(
    dag_id='example_af2_dag',
    start_date=datetime(2025, 12, 10),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'dynamic-mapping', 'traditional-operators'],
) as dag:
    
    files_to_process = ['sales_2025.csv', 'customers.json', 'products.xml', 'inventory.parquet']
    
    start_with_process_files = PythonOperator.partial(
        task_id='start_with_process_files',
        python_callable=process_file,
        # These parameters are STATIC - same for all mapped instances
    ).expand(
        # This parameter is DYNAMIC - different for each instance 
        op_kwargs=[{'file_name': f,'base_path': f'/data/raw/{f}', 'date': '{{ ds }}'} for f in files_to_process]
    )
    
    echo_commands = [
        'echo "Task 1: Starting data pipeline"',
        'echo "Task 2: Extracting data"',
        'echo "Task 3: Transforming data"',
        'echo "Task 4: Loading data"',
        'echo "Task 5: Pipeline complete"',
    ]
    
    pipeline_steps = BashOperator.partial(
        task_id='pipeline_steps',
    ).expand(
        bash_command=echo_commands
    )

    # ============================================
    # TASK DEPENDENCIES
    # ============================================
    # Set up the flow
    start_with_process_files >> pipeline_steps