from .pollux_exe.utils import NodeInfo
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
        # TODO: Check if node_info is indeed server_map
        nodes = self._get_pollux_nodes(node_info)



        # call pollux.optimize(jobs, nodes, base_allocations)


        schedule_info = dict()
        return schedule_info

    def _get_pollux_nodes(self, node_info):
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

        nodes = dict()

        for node_id, node_data in node_info.keys(), node_info.values():
            # TODO: Can we ignore cpu/pods for the resources map below?
            # TODO: How does preemptible work? Can we get this value somewhere in Blox?
            nodes[node_id] = NodeInfo(resources={"gpu": node_data["numGPUs"]}, preemptible=False)

        return nodes
