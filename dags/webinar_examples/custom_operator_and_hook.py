from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your custom operator
from include.custom_operators.bhp_operator import BHPOperator
from include.custom_hooks.bhp_hook import BHPApiHook, BHPFileHook


def use_custom_api_hook():
    """Function that uses the custom API hook."""
    # Initialize the hook
    hook = BHPApiHook(api_conn_id='http_default')  # Use existing http connection
    
    try:
        # Use the hook
        hook.log.info("Using custom API hook...")
        
        # Example: Get data (this will fail if endpoint doesn't exist, but shows usage)
        # data = hook.get_data('api/v1/data', params={'limit': 10})
        
        hook.log.info("Custom hook executed successfully!")
        
    except Exception as e:
        hook.log.error(f"Hook execution failed: {e}")
    
    finally:
        hook.close()


def use_custom_file_hook():
    """Function that uses the custom file hook."""
    hook = BHPFileHook()
    
    # Write a test file
    hook.write_file('test_output.txt', 'Hello from custom hook!')
    hook.log.info("File written successfully!")
    
    # Read it back
    content = hook.read_file('test_output.txt')
    hook.log.info(f"File content: {content}")


with DAG(
    dag_id='test_custom_operator_and_hooks',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'Avery',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['custom', 'operator', 'hook', 'test'],
) as dag:

    # ============================================
    # Test Custom Operator
    # ============================================
    
    # Use the custom BHPOperator
    custom_bash_task = BHPOperator(
        task_id='custom_bash_with_logging',
        bash_command='echo "Hello from custom operator!" && date && sleep 2',
        log_prefix="[MyCustomPrefix]",  # Custom parameter
    )
    
    # Another example with different command
    custom_bash_task_2 = BHPOperator(
        task_id='custom_bash_ls',
        bash_command='ls -la /tmp && echo "Directory listing complete"',
        log_prefix="[DirectoryCheck]",
    )
    
    # ============================================
    # Test Custom Hooks
    # ============================================
    
    test_api_hook = PythonOperator(
        task_id='test_api_hook',
        python_callable=use_custom_api_hook,
    )
    
    test_file_hook = PythonOperator(
        task_id='test_file_hook',
        python_callable=use_custom_file_hook,
    )
    
    # ============================================
    # Task Dependencies
    # ============================================
    
    custom_bash_task >> custom_bash_task_2 >> [test_api_hook, test_file_hook]