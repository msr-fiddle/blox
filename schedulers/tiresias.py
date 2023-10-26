from .scheduler_policy import SchedulingPolicy
import pandas as pd
from operator import getitem

from typing import Optional


class Tiresias(SchedulingPolicy):
    """
    Implements Least attained service scheduler
    """

    def __init__(self, args, num_queus=1, service_per_queue=[1]):
        """
        Use this to hold any extra state the scheduler wants to hold
        """
        self.metric_to_track = ["per_iter_time", "attained_service"]
        self.default_metric_value = [0, 0]
        self.queues = [[] for _ in range(num_queus)]
        self.service_per_queue = service_per_queue

    @SchedulingPolicy.copy_arguments
    def schedule(
        self,
        job_dict: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        global_placement_policy: Optional[str] = None,
    ) -> dict:
        # print(f"Job dict las {job_dict}")
        sorted_job_order = sorted(
            job_dict.items(),
            key=lambda x: (
                x[1]["job_priority"],
                x[1]["tracked_metrics"]["attained_service"],
            ),
        )
        schedule_info = dict()
        queue_to_put_a_job = 0
        schedule_info["job_order"] = sorted_job_order
        schedule_info["run_all_jobs"] = False
        for jobs in sorted_job_order:
            #  go over jobs and put them in queues
            if (
                jobs[1]["tracked_metrics"]["attained_service"]
                < self.service_per_queue[queue_to_put_a_job]
            ):
                self.queues[queue_to_put_a_job].append(jobs)
        # final order
        combine_across_all_list = list()
        for q in self.queues:
            combine_across_all_list.extend(q)
        return schedule_info
