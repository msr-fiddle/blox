import pandas as pd
import copy
from typing import Tuple, List


class SynergyPlacement(object):
    def __init__(self):
        pass

    def place_jobs(
        self,
        active_jobs: dict,
        new_job_schedule: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        **kwargs,
    ) -> dict:
        """
        parses the sorted_jobs dictionary and calls relevant placement policy

        """
        job_order = new_job_schedule["job_order"]
        jobs_to_terminate = list()
        job_to_launch = dict()
        jobs_this_round = list()
        num_free_gpus = find_num_free_GPUs(gpu_df)
        # for job in job_order:
        # job_gpu_deficit = (
        # active_jobs[jid]["job_gpu_demand"] - active_jobs["allocated_gpu"]
        # )
        # if job_gpu_deficit > 0 and num_free_gpus > 0:
        # jobs_this_round.append(job)
        # num_free_gpus -= job_gpu_deficit

        num_free_gpus -= job_gpu_deficit
        for job in job_order:
            job_gpu_deficit = (
                active_jobs[jid]["job_gpu_demand"] - active_jobs["allocated_gpu"]
            )
            if job_gpu_deficit > 0 and num_free_gpus >= job_gpu_deficit:
                # start allocation
                demand_vector = active_jobs["demand_vector"]

            # this is synergy random policy
            job_demand_vector_gpu_norm = gpu_normalized_vector(job_demand_vector)
            free_available_gpus = find_free_GPUs(gpu_df)
            gpus_to_allocate, res_map = _top_synergy_gpus(
                job_demand_vector_gpu_norm,
                active_jobs[jid]["job_gpu_demand"],
                free_available_gpus,
            )

        for node_id in available_gpus


def gpu_normalized_vector(vector):
    return [item / vector[0] for item in vector]


def _top_synergy_gpus(job_demand_vector, job_gpu_demand, available_gpus):
    """
    Find free GPUs for training 
    job_demand_vector (list) : Job demand vector from training
    job_gpu_demand (int) : Number of GPUs requested by the job
    available_gpus : List of available GPUs
    """
    for node_id in available_gpus:
        for gpu_id in available_gpus[node_id]:
            if fits_in_server(gpu_id, norm_demand_vector):
                pass


def fits_in_server(gpu_id, norm_demand_vector, server=None):
    """
    If the GPU or server can fit 
    """

    if gpu is not None:
        free_vector = list()
    elif server is not None:
        free_vector = list()
    



def find_free_GPUs(gpu_df: pd.DataFrame) -> dict:
    """
    Find the nodeID's which have free GPUs
    Args:
    gpu_df : DataFrame consisting of information about GPUs
    Returns:
    dict: {Node_ID: [list of free GPUs]}
    """
    return (
        gpu_df.loc[gpu_df["IN_USE"] == False]
        .groupby("Node_ID")["GPU_ID"]
        .apply(list)
        .to_dict()
    )


def find_num_free_GPUs(gpu_df: pd.DataFrame) -> dict:
    """
    Find the number of free GPU's
    Args:
    gpu_df : DataFrame consisting of information about GPUs
    Returns:
    int : Number of free GPUs
    """

    temp_data = (
        gpu_df.loc[gpu_df["IN_USE"] == False]
        .groupby("Node_ID")["GPU_ID"]
        .apply(list)
        .to_dict()
    )

    num_gpus = 0
    for per_node in temp_data:
        num_gpus += len(temp_data[per_node])

    return num_gpus
