from .scheduler_policy import SchedulingPolicy
import pandas as pd
from operator import getitem
import os
from typing import Optional


class Optimus(SchedulingPolicy):
    """
    Implements Shortest remaining time first
    """

    def __init__(self, args):
        """
        Use this to hold any extra state the scheduler wants to hold
        """
        self.metric_to_track = ["per_iter_time", "attained_service"]
        self.default_metric_value = [0, 0]
        # self.marginal_utility = {'resnet50':
        pass

    @SchedulingPolicy.copy_arguments
    def schedule(
        self,
        job_dict: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        global_placement_policy: Optional[str] = None,
    ) -> dict:
        """
        Optimus Scheduler. Assuming the job closest to convergence is known. Similar to Pollux's, Optimus Oracle
        """
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
        # NOTE: Borrowed from https://github.com/kzhang28/Optimus/blob/cea8c8bb39da493b5a45cdce625fe3c225c1793c/measurement/training-speed/measure-speed-placements.py#L170
        for jobs in sorted_job_order:
            jobs[1]["job_gpu_demand"] = jobs[1]["num_GPUs"]
            jobs[1]["total_gpus"] = os.environ["sched_load"]
        total_gpu_demand = self._total_gpu_demand(sorted_job_order)
        free_gpus = self._get_free_gpus(gpu_df) - total_gpu_demand
        # allocate more GPUs if there are GPUs left
        # the jobs are already stored in the order they will converge
        if len(sorted_job_order) > 0:
            while free_gpus > 0:
                # print("Outside")
                # print(free_gpus)
                for jobs in sorted_job_order:
                    # keep adding the number of GPUs needed by each job
                    jobs[1]["job_gpu_demand"] += 1
                    free_gpus = free_gpus - 1
                    # print(free_gpus)
                    if free_gpus == 0:
                        break

        return schedule_info

    def _total_gpu_demand(self, sorted_job_order):
        """
        Sum up the GPU demand
        """
        gpu_demand = 0
        for jobs in sorted_job_order:
            gpu_demand += int(jobs[1]["job_gpu_demand"])
        return gpu_demand

    def _get_free_gpus(self, gpu_df: pd.DataFrame) -> dict:
        """
        Find the nodeID's which have free GPUs
        Args:
        gpu_df : DataFrame consisting of information about GPUs
        Returns:
        int
        """
        free_gpus = (
            gpu_df.loc[gpu_df["IN_USE"] == False]
            .groupby("Node_ID")["GPU_ID"]
            .apply(list)
            .to_dict()
        )
        number_free_gpus = sum([len(free_gpus[x]) for x in free_gpus])
        return number_free_gpus
