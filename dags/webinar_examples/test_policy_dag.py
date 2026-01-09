from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ✅ This will PASS policy
with DAG(
    dag_id='valid_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    tags=['test', 'example'],
    default_args={
        'owner': 'data-team',
        'retries': 2,
    },
    catchup=False,
) as dag:
    
    task = BashOperator(
        task_id='run_test',
        bash_command='echo "Hello World"',
    )