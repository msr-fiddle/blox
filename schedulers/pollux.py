from .scheduler_policy import SchedulingPolicy
import pandas as pd
from operator import getitem

from typing import Optional


class Pollux(SchedulingPolicy):
    """
    TODO: Implements Pollux scheduler scheduler
    """

    def __init__(self, args):
        """
        Use this to hold any extra state the scheduler wants to hold
        """
        self.metric_to_track = ["per_iter_time", "attained_service"]
        self.default_metric_value = [0, 0]

    @SchedulingPolicy.copy_arguments
    def schedule(
        self,
        job_dict: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        global_placement_policy: Optional[str] = None,
    ) -> dict:
        # print(f"Job dict las {job_dict}")
        # create jobs, base_allocations

        

        # create nodes



        # call pollux.optimize(jobs, nodes, base_allocations)


        return schedule_info
