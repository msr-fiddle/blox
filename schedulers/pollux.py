import sys
sys.path.append("..")
from .pollux_exe.utils import NodeInfo
from .scheduler_policy import SchedulingPolicy
from .pollux_exe.pollux_engine import PolluxPolicy
from .pollux_exe.utils import JobInfo
from placement.placement import find_free_GPUs
from blox.blox_manager import _free_gpu_by_jobid
import pandas as pd
from operator import getitem

from typing import Optional, Dict, Union, Any, List


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
        node_info: dict, # XY: cluster_state.server_map
        gpu_df: pd.DataFrame, # XY: cluster_state.gpu_df
        allocations: dict, # XY added
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
        nodes = dict()

        for node_id, node_data in node_info.items():
            # TODO: Can we ignore cpu/pods for the resources map below?
            # TODO: How does preemptible work? Can we get this value somewhere in Blox?
            nodes[node_id] = NodeInfo(resources={"nvidia.com/gpu": node_data["numGPUs"], "cpu": node_data["numCPUcores"]}, preemptible=False)

        # call pollux.optimize(jobs, nodes, base_allocations)
        schedule_info = dict()
        schedule_info ["allocations"] = dict()
        schedule_info["to_suspend"] = list() # list of jid
        schedule_info["to_launch"] = dict() # jid -> ...

        if job_infos:
            allocations = {k: v for k, v in allocations.items() if k in job_infos}
            results = self.engine.optimize(job_infos, nodes,
                                           allocations, nodes[0])
            new_allocations, desired_nodes = results
            # used_gpus = collections.Counter(sum(allocations.values(), []))
            # assert all(val <= node_infos[key].resources["nvidia.com/gpu"]
            #            for key, val in used_gpus.items())

            schedule_info["allocations"] = new_allocations

            """
            Assign to_suspend, to_launch
            """
            # XY: is there a way to adjust allocation rather than totally turn off before turning on?
            #     is it possible to turn off only some of the GPUs of a job rather than turning all off?
            # if jid not in new_allocations:
            #     schedule_info["to_suspend"].append(jid)

            for jid in job_dict:
                if new_allocations.get(jid) == allocations.get(jid):
                    continue
                if allocations.get(jid) and len(allocations.get(jid)) > 0:
                    schedule_info["to_suspend"].append(jid)
                if len(new_allocations.get(jid)) == 0:
                    continue

                alloc = new_allocations.get(jid)
                placement = {} # XY: record number of gpus allocated on each node
                for i in range(len(alloc)):
                    if i == 0 or alloc[i] != alloc[i - 1]:
                        placement[alloc[i]] = 1
                    else:
                        placement[alloc[i]] += 1

                gpu_df_copy = gpu_df.copy()
                # update gpu_df_copy to as if jid is not running
                _free_gpu_by_jobid(jid, gpu_df_copy)
                gpus_for_job = list()  # XY: list of GPU IDs
                free_gpus = find_free_GPUs(gpu_df_copy)

                for node_id in placement:
                    gpus_for_job.extend(free_gpus[node_id][:placement[node_id]])
                schedule_info["to_launch"][jid] = gpus_for_job

            # to_launch will likely require nodes object

        return schedule_info

"""
Converts Blox's 'node_info' to Pollux's 'nodes'

node_info is a ClusterState object which contains the following fields:
1. server_map - Maps the ID of each node in the cluster to its data
- Example of server_map:
        {
            0: {"numGPUs": 3, "gpuUUIDs": "ki78b3\nuife3b\n3bi7fr", ipaddr: 10.0.0.1},
            1: {"numGPUs": 2, "gpuUUIDs": "je97k4\nery26h", ipaddr: 10.0.0.2}
        }
2. node_counter - keeps incrementing when new nodes are added to cluster, used for setting Node ID
3. gpu_number - keeps incrementing when new nodes -> new GPUs are added to cluster, used for setting GPU ID (not UUID)
4. gpu_df - a list of GPUs, where each GPU is stored as a dictionary of values
- Example of gpu_df: (Below, GPU_ID is the ID of GPU in cluster, Local_GPU_ID is the ID of the GPU in the individual node)
        [
            {"GPU_ID": 0, "Node_ID": 0, "GPU_UUID": ki78b3, "Local_GPU_ID": 0, "IP_addr": 10.0.1.1, "IN_USE": False, "JOB_IDS": None},
            {"GPU_ID": 1, "Node_ID": 0, "GPU_UUID": uife3b, "Local_GPU_ID": 1, "IP_addr": 10.0.1.1, "IN_USE": True, "JOB_IDS": None},
            {"GPU_ID": 2, "Node_ID": 0, "GPU_UUID": 3bi7fr, "Local_GPU_ID": 2, "IP_addr": 10.0.1.1, "IN_USE": False, "JOB_IDS": None},
            {"GPU_ID": 3, "Node_ID": 1, "GPU_UUID": je97k4, "Local_GPU_ID": 0, "IP_addr": 10.0.1.2, "IN_USE": False, "JOB_IDS": None}
            {"GPU_ID": 4, "Node_ID": 1, "GPU_UUID": ery26h, "Local_GPU_ID": 1, "IP_addr": 10.0.1.2, "IN_USE": True, "JOB_IDS": None}
        ]
5. time - Current time, starts from 0, increases by round time (say 80) i.e. time taken for each round (iteration in pollux_scheduler)
6. cluster_stats - Maps time to cluster stats at that time.
- Example of cluster_stats: (Below, free_gpus is # of GPUs which are not IN_USE,
                            gpu_demand is the sum of job_state.active_jobs[jobid]["num_gpus"] over all active jobs)
        {
            0: {"total_jobs": 10, "jobs_in_queue": 6, "jobs_running": 4, "free_gpus": 3, "gpu_demand": 15},
            80: {"total_jobs": 11, "jobs_in_queue": 7, "jobs_running": 4, "free_gpus": 3, "gpu_demand": 17},
            160: {"total_jobs": 11, "jobs_in_queue": 6, "jobs_running": 5, "free_gpus": 3, "gpu_demand": 17}
        }

nodes is a map from node keys to 'NodeInfo' objects. Each NodeInfo object has the following fields:
Example of nodes map:
{
    0: NodeInfo(resources={"gpu": 10, "cpu": 500, "pods": 32}, preemptible=False),
    1: NodeInfo(resources={"gpu": 15, "cpu": 300, "pods": 16}, preemptible=True)
}
"""
