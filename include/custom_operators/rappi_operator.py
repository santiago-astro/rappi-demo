from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults


class RappiOperator(BashOperator):
    """
    Custom BashOperator with enhanced logging at each execution step.
    
    This operator extends the standard BashOperator and adds detailed
    logging during pre-execution, execution, and post-execution phases.
    """
    
    @apply_defaults
    def __init__(
        self,
        bash_command,
        log_prefix="[CustomLog]",  # Custom parameter
        **kwargs
    ):
        super().__init__(bash_command=bash_command, **kwargs)
        self.log_prefix = log_prefix
    
    def pre_execute(self, context):
        """Runs before the main execute method."""
        # Write to Airflow task logs
        self.log.info(f"{self.log_prefix} PRE-EXECUTION STEP")
        self.log.info(f"Task ID: {self.task_id}")
        self.log.info(f"DAG ID: {context['dag'].dag_id}")
        self.log.info(f"Execution Date: {context['execution_date']}")
        self.log.info(f"Bash Command: {self.bash_command}")
        self.log.info(f"Environment Variables: {len(self.env or {})} variables set")
        
        # Call parent's pre_execute if it exists
        if hasattr(super(), 'pre_execute'):
            super().pre_execute(context)
    
    def execute(self, context):
        """Main execution method - runs the bash command."""
        # Write to Airflow task logs
        self.log.info(f"{self.log_prefix} EXECUTION STEP")
        self.log.info("Starting bash command execution...")
        
        try:
            # Execute the parent BashOperator's execute method
            result = super().execute(context)
            
            self.log.info("Bash command completed successfully! ✅")
            self.log.info("Return code: 0")
            
            return result
            
        except Exception as e:
            self.log.error("Bash command failed! ❌")
            self.log.error(f"Error: {str(e)}")
            raise
                    
    
    def post_execute(self, context, result=None):
        """Runs after the main execute method."""
        # Write to Airflow task logs
        
        self.log.info(f"{self.log_prefix} POST-EXECUTION STEP")
        
        self.log.info(f"Task completed for: {self.task_id}")
        self.log.info(f"Result: {result}")
        self.log.info(f"Task State: {context['task_instance'].state}")
        self.log.info("Performing cleanup operations...")
        self.log.info("All done! 🎉")
        
        
        # Call parent's post_execute if it exists
        if hasattr(super(), 'post_execute'):
            super().post_execute(context, result)