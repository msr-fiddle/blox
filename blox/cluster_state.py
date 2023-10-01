import sys
import time
import copy
import grpc
import json
import logging
import argparse
import pandas as pd
import time
from concurrent import futures

from typing import Tuple, List

# import scheduler
# import placement

from blox_manager import BloxManager

# from profile_parsers import pparsers


class ClusterState(object):
    """
    Keep tracks of cluster state
    """

    def __init__(self, args: argparse.ArgumentParser) -> None:
        # self.blr = blox_resource_manager
        # keeps a map of nodes
        self.server_map = dict()
        # keeps the count of node
        self.node_counter = 0
        # number of GPUs
        self.gpu_number = 0
        # gpu dataframe for easy slicing
        self.gpu_df = pd.DataFrame(
            columns=[
                "GPU_ID",
                "Local_GPU_ID",
                "Node_ID",
                "IP_addr",
                "IN_USE",
                "JOB_IDS",
            ]
        )
        self.simulator_time = 0
        self.cluster_stats = dict()

    # def get_new_nodes(self):
    # """
    # Fetch any new nodes which have arrived at the scheduler
    # """
    # new_nodes = self.blr.rmserver.get_new_nodes()
    # return new_nodes

    def _add_new_machines(self, new_nodes: List[dict]) -> None:
        """
        Pops information of new machines and keep track of them

        new_node : list of new nodes registered with the resource manager
        """
        while True:
            try:
                node_info = new_nodes.pop(0)
                self.server_map[self.node_counter] = node_info
                numGPUs_on_node = node_info["numGPUs"]
                if numGPUs_on_node > 0:
                    gpuID_list = list()
                    for local_gpu_id in range(numGPUs_on_node):
                        gpuID_list.append(
                            {
                                "GPU_ID": self.gpu_number,
                                "Node_ID": self.node_counter,
                                "Local_GPU_ID": local_gpu_id,
                                "IP_addr": node_info["ipaddr"],
                                "IN_USE": False,
                                "JOB_IDS": None,
                            }
                        )
                        self.gpu_number += 1
                    self.gpu_df = self.gpu_df.append(gpuID_list)
                    self.node_counter += 1
            except IndexError:
                break

    def update(self, new_nodes):
        """
        Updates cluster state by fetching new nodes
        Args:
            None
        Returns:
            new_nodes : List of new nodes
        """
        # getting new updates
        # new_nodes = self.blr.rmserver.get_new_nodes()

        if len(new_nodes) > 0:
            self._add_new_machines(new_nodes)
        return new_nodes
