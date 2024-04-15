from .scheduler_policy import SchedulingPolicy
from .pollux_exe.pollux_engine import PolluxPolicy
from .pollux_exe.utils import JobInfo
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
        self.engine = PolluxPolicy()

    @SchedulingPolicy.copy_arguments
    def schedule(
        self,
        job_dict: dict, # XY: job_state.active_jobs
        simulator_time, # XY added
        node_info: dict, # XY: cluster_state.server_map, use this as self.allocations in Pollux repo
        gpu_df: pd.DataFrame, # XY: cluster_state.gpu_df
        global_placement_policy: Optional[str] = None,
    ) -> dict:
        # print(f"Job dict las {job_dict}")
        # create jobs, base_allocations
        job_infos = {}
        for jid in job_dict:
            if simulator_time >= job_dict[jid]["job_arrival_time"]:
                job = job_dict[jid]["tracked_metrics"]["pollux_metrics"]
                job_info = JobInfo(
                    resources={"nvidia.com/gpu": 1},
                    speedup_fn=job.get_speedup_fn(),
                    creation_timestamp=job.submission_time,
                    attained_service=job.attained_service,
                    min_replicas=0,
                    max_replicas=min(max(2 * job.max_profiled_replicas, 1), 64,  # simulator can't handle more.
                                     job.application.max_batch_size // job.application.min_local_bsz),
                    preemptible=True,
                )
                if job.application.name == "ncf":
                    job_info.max_replicas = 1
                job_info.num_restarts = job.num_restarts or 0
                job_info.age = simulator_time - job.submission_time
                job_infos[job.name] = job_info

        # create nodes
        nodes = {}

        # call pollux.optimize(jobs, nodes, base_allocations)
        schedule_info = dict()
        schedule_info ["allocations"] = dict()
        schedule_info["to_suspend"] = dict()
        schedule_info["to_launch"] = dict()

        if job_infos:
            results = self.engine.optimize(job_infos, nodes,
                                           node_info, nodes[0])
            allocations, desired_nodes = results
            # used_gpus = collections.Counter(sum(allocations.values(), []))
            # assert all(val <= node_infos[key].resources["nvidia.com/gpu"]
            #            for key, val in used_gpus.items())

            schedule_info["allocations"] = allocations

            """
            Assign to_suspend, to_launch
            """

        return schedule_info
