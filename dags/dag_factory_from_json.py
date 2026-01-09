from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import os


# ============================================
# Load configurations from JSON
# ============================================

# Get the path to the config file
config_path = os.path.join(
    os.path.dirname(__file__),
    '..',
    'include',
    'factory',
    'dag_configs.json'
)

# Load the JSON config
with open(config_path, 'r') as f:
    DAG_CONFIGS = json.load(f)


# ============================================
# DAG Factory Function
# ============================================

def create_dag_from_json(config):
    """Create a DAG from JSON configuration."""
    
    default_args = {
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    
    dag = DAG(
        dag_id=config['dag_id'],
        description=config['description'],
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule_interval=config['schedule'],
        catchup=False,
        tags=config['tags'],
    )
    
    with dag:
        tasks = []
        
        for task_config in config['tasks']:
            task = BashOperator(
                task_id=task_config['task_id'],
                bash_command=task_config['command'],
            )
            tasks.append(task)
        
        # Set dependencies
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]
    
    return dag


# ============================================
# Generate all DAGs from JSON
# ============================================

for config in DAG_CONFIGS:
    dag_id = config['dag_id']
    globals()[dag_id] = create_dag_from_json(config)