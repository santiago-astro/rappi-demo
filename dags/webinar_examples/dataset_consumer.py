from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import os


# ============================================
# STEP 1: Reference the SAME Dataset
# ============================================
# IMPORTANT: The URI must match EXACTLY

sales_data_dataset = Dataset("file:///test_output.txt")


# ============================================
# STEP 2: Function that "consumes" data
# ============================================

def process_sales_data(**context):
    """
    Process the sales data that was generated.
    This runs automatically when the dataset is updated!
    """
    
    # Check if the file exists
    filepath = '/tmp/sales_data.json'
    
    if not os.path.exists(filepath):
        print("⚠️ No data file found!")
        return None
    
    # Read the data
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print("=" * 60)
    print("📥 PROCESSING SALES DATA")
    print("=" * 60)
    print(f"Date: {data['date']}")
    print(f"Total Sales: ${data['total_sales']:,}")
    print(f"Orders: {data['orders']}")
    print(f"Top Product: {data['top_product']}")
    print(f"Generated At: {data['generated_at']}")
    print("=" * 60)
    
    # Do some processing (simulation)
    daily_average = data['total_sales'] / data['orders']
    print(f"💰 Average Order Value: ${daily_average:.2f}")
    print("=" * 60)
    
    return {
        'processed': True,
        'average_order_value': daily_average
    }


# ============================================
# STEP 3: Create the Consumer DAG
# ============================================

with DAG(
    dag_id='dataset_consumer',
    start_date=datetime(2024, 1, 1),
    
    # ============================================
    # STEP 4: schedule=[dataset] - THE MAGIC!
    # ============================================
    # Instead of '@daily' or cron, we use a list of datasets
    # This DAG will trigger whenever ANY of the datasets updates
    
    schedule=[sales_data_dataset],  # ← KEY: Triggered by dataset!
    
    catchup=False,
    default_args={
        "retries": 3,
        "owner": "Avery",
    },
    tags=['dataset', 'consumer'],
    description='Processes sales data when it becomes available',
) as consumer_dag:
    
    start = BashOperator(
        task_id='start',
        bash_command='''
            echo "🔔 Dataset update detected!"
            echo "📊 Starting sales data processing..."
        ''',
    )
    
    process_data = PythonOperator(
        task_id='process_sales_data',
        python_callable=process_sales_data,
    )
    
    finish = BashOperator(
        task_id='finish',
        bash_command='echo "✅ Sales data processing complete!"',
    )
    
    # Dependencies
    start >> process_data >> finish