from airflow.models import DAG
from airflow.exceptions import AirflowException


def dag_policy(dag: DAG):
    """Enforce DAG policies."""
    
    # Require tags
    if not dag.tags:
        raise AirflowException(
            f"❌ DAG '{dag.dag_id}' must have at least one tag. "
            f"Add tags=['your-tag'] to your DAG definition."
        )
    
    # Require owner
    if not dag.owner or dag.owner == 'airflow':
        raise AirflowException(
            f"❌ DAG '{dag.dag_id}' must have a valid owner."
        )
    
    print(f"✅ DAG '{dag.dag_id}' passed policy checks (tags: {dag.tags})")


def task_policy(task):
    """Enforce task-level policies (optional)."""
    
    # Example: Require retries for all tasks
    if task.retries < 1:
        raise AirflowException(
            f"❌ Task '{task.task_id}' in DAG '{task.dag_id}' must have at least 1 retry."
        )