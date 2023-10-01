from .scheduler_policy import SchedulingPolicy
import pandas as pd
from operator import getitem

from typing import Optional


class Srtf(SchedulingPolicy):
    """
    Implements Shortest remaining time first
    """

    def __init__(self, args):
        """
        Use this to hold any extra state the scheduler wants to hold
        """
        self.metric_to_track = ["per_iter_time", "attained_service"]
        self.default_metric_value = [0, 0]
        pass

    @SchedulingPolicy.copy_arguments
    def schedule(
        self,
        job_dict: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        global_placement_policy: Optional[str] = None,
    ) -> dict:

        for job in job_dict:
            job_dict[job]["time_remaining"] = (
                job_dict[job]["job_iteration_time"]
                * job_dict[job]["job_total_iteration"]
                - job_dict[job]["tracked_metrics"]["attained_service"]
            )
        sorted_job_order = sorted(
            job_dict.items(),
            key=lambda x: (x[1]["job_priority"], x[1]["time_remaining"]),
        )

        schedule_info = dict()
        schedule_info["job_order"] = sorted_job_order
        schedule_info["run_all_jobs"] = False

        return schedule_info
