from datetime import timedelta
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable, DagRunInfo, DataInterval, TimeRestriction
from pendulum import DateTime


class EveryTwoHoursTimetable(Timetable):
    """
    Simplest timetable: runs every 2 hours.
    """
    
    # ============================================
    # IMPORTANT: Add serialize/deserialize
    # ============================================
    
    def serialize(self) -> dict:
        """Serialize the timetable (required for DAG serialization)."""
        return {}  # No config needed for this simple timetable
    
    @classmethod
    def deserialize(cls, data: dict) -> "EveryTwoHoursTimetable":
        """Deserialize the timetable (required for DAG serialization)."""
        return cls()
    
    # ============================================
    # Core methods
    # ============================================
    
    @property
    def summary(self) -> str:
        """This text appears in the Airflow UI."""
        return "Every 2 hours"
    
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        """Calculate when the DAG should run next."""
        
        # First run?
        if last_automated_data_interval is None:
            if restriction.earliest is None:
                return None
            next_run_time = restriction.earliest
        else:
            # Add 2 hours to the last run
            next_run_time = last_automated_data_interval.end.add(hours=2)
        
        # Check if we've passed the end date
        if restriction.latest is not None and next_run_time > restriction.latest:
            return None
        
        # Calculate the data interval
        interval_start = next_run_time.subtract(hours=2)
        interval_end = next_run_time
        
        return DagRunInfo(
            run_after=next_run_time,
            data_interval=DataInterval(
                start=interval_start,
                end=interval_end,
            ),
        )
    
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Handle manually triggered runs."""
        start = run_after.subtract(hours=2)
        end = run_after
        return DataInterval(start=start, end=end)

# ============================================
# CRITICAL: Register as plugin
# ============================================
class EveryTwoHoursTimetablePlugin(AirflowPlugin):
    name = "simple_timetable_plugin"
    timetables = [EveryTwoHoursTimetable]

