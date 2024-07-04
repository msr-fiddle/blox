import pandas as pd
import copy
from typing import Tuple, List


class JobPlacement(object):
    def __init__(self, args):
        pass

    @staticmethod
    def copy_arguments(function):
        def function_wrapper(
            self, job_state, cluster_state, new_job_schedule, **kwargs
        ):
            return function(
                self,
                job_state.active_jobs,
                copy.deepcopy(new_job_schedule),
                copy.deepcopy(cluster_state.server_map),
                copy.deepcopy(cluster_state.gpu_df),
                **copy.deepcopy(kwargs),
            )

        return function_wrapper

    @copy_arguments.__func__
    def place(
        self,
        active_jobs: dict,
        new_job_schedule: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        **kwargs,
    ) -> dict:
        """
        parses the sorted_jobs dictionary and calls relevant placement policy

        # CAUTION: This function makes in place changes to active jobs and
        # gpu_df

        """
        job_order = new_job_schedule["job_order"]
        scheduler = new_job_schedule.get("scheduler")
        jobs_to_terminate = list()
        job_to_launch = dict()
        launched_job_ids = list()
        # go over jobs in job order
        if scheduler == "Gavel":
            for idx, job_priority_sorted in enumerate(job_order):
                job_id, gpu_preference = list(job_priority_sorted.keys())[0]
                job = active_jobs[job_id]
                found = False
                if job["is_running"] == True:
                    if job["running_accel"] == gpu_preference:
                        # nothing to do here
                        continue
                    else:
                        # need to terminate this job trying to launch on
                        # different accelerator
                        jobs_to_terminate.append(job_id)
                        job["is_running"] = False
                        delete_job_by_id(gpu_df, job_id)

                if job_id in launched_job_ids:
                    # already launched the same ID in this round
                    continue
                if job["is_running"] == False:
                    # need to find placement only if job is not running
                    place_consolidated = (
                        job.get("placement_preference") == "consolidated"
                    )

                    free_gpus = find_free_GPUs_by_type(gpu_df, gpu_preference)
                    if place_consolidated:
                        placement, found = self._consolidated_placement(job, free_gpus)
                    else:
                        placement, found = self._scattered_placement(job, free_gpus)
                    if not found:
                        # no free GPUs
                        # find the GPU with same GPU preference in the reverse
                        # order of priority
                        for rev_idx in range(1, len(active_jobs) - idx):
                            potential_terminate_job_pair = job_order[-rev_idx]
                            if potential_terminate_job_pair[1] != gpu_preference:
                                # Job doesn't have the same preference
                                continue
                            else:
                                # the job has the same preference

                                # need to check if it is running
                                # and if it is running on the same as current
                                # preference
                                potential_terminate_job_info = active_jobs[
                                    potential_terminate_job_pair[0]
                                ]
                                if (
                                    potential_terminate_job_info["is_running"] == True
                                ) and (
                                    potential_terminate_job_info["running_accel"]
                                    == gpu_preference
                                ):
                                    # only terminate in case the training is
                                    # also happening on the same GPU as the
                                    # preference
                                    jobs_to_terminate.append(
                                        potential_terminate_job_pair[0]
                                    )
                                    potential_terminate_job_info["is_running"] = False
                                    # freeing up GPUs
                                    delete_job_by_id(
                                        gpu_df, potential_terminate_job_pair[0]
                                    )
                                    free_gpus = find_free_GPUs_by_type(
                                        gpu_df, gpu_preference
                                    )

                                    if place_consolidated:
                                        placement, found = self._consolidated_placement(
                                            job, free_gpus
                                        )
                                    else:
                                        placement, found = self._scattered_placement(
                                            job, free_gpus
                                        )

                                    if found:
                                        # we found the placement
                                        break

                                    # terminate this job
                                else:
                                    # job matching not found
                                    continue
                if found:
                    launched_job_ids.append(job_id)
                    job_to_launch[job_id] = placement
                    active_jobs[jid]["running_accel"] = gpu_preference
                    mark_gpu_in_use(gpu_df, placement, job_id)
                else:
                    break

            return (jobs_to_terminate, jobs_to_launch)

            # accel_sorted_by_pref - key: gpu_type, val: list of job ids sorted
            # by decreasing preference

        if scheduler is None:
            running_jobs = 0
            new_scheduled_jobs = 0
            jobs_to_schedule = 0
            for idx, job_id in enumerate(job_order):
                job_id, _ = job_id
                job = active_jobs[job_id]
                found = False
                if job["is_running"] == True:
                    # move to lower priority jobs
                    running_jobs += 1
                    continue
                if job["is_running"] == False:
                    # need to find placement only if job is not running
                    place_consolidated = (
                        job.get("placement_preference") == "consolidated"
                    )

                    # first checking if there are free GPUs
                    free_gpus = find_free_GPUs(gpu_df)
                    if place_consolidated:
                        placement, found = self._consolidated_placement(job, free_gpus)
                    else:
                        placement, found = self._scattered_placement(job, free_gpus)
                    # next checking if there are lower priority jobs which have
                    if not found:
                        # no free GPUs
                        # need to see if there are lower priority jobs which can be
                        # terminated and placement can be found then

                        for rev_idx in range(1, len(active_jobs) - idx):
                            potential_job_to_terminate = active_jobs[
                                job_order[-rev_idx][0]
                            ]
                            if potential_job_to_terminate["is_running"] == True:
                                # terminate this job
                                jobs_to_terminate.append(job_order[-rev_idx][0])
                                potential_job_to_terminate["is_running"] = False
                                # freeing up GPUs
                                delete_job_by_id(gpu_df, job_order[-rev_idx][0])
                                free_gpus = find_free_GPUs(gpu_df)
                                if place_consolidated:
                                    placement, found = self._consolidated_placement(
                                        job, free_gpus
                                    )
                                else:
                                    placement, found = self._scattered_placement(
                                        job, free_gpus
                                    )
                                if found:
                                    # we found an assignment
                                    # print(
                                    # f"Placed {job_id} by determining to terminate{job_order[-rev_idx][0]}"
                                    # )
                                    break
                if found:
                    new_scheduled_jobs += 1
                    job_to_launch[job_id] = placement
                    # update manual-pipeline-list for bert and gpt
                    mark_gpu_in_use(gpu_df, placement, job_id)
                else:
                    # print(f"New Jobs scheduled {new_scheduled_jobs}")
                    # print(f"Jobs previously running {running_jobs}")
                    # print(f"Jobs terminated {len(jobs_to_terminate)}")
                    # print(f"Jobs in queue {len(job_order)-idx}")
                    break
            return (jobs_to_terminate, job_to_launch)

    def _consolidated_placement(
        self, job_param: dict, free_gpus: dict
    ) -> Tuple[list, bool]:
        """
        Find a consolidated placement
        Args:
        job_param: Job Param configuration
        free_gpus: Dict of free GPUs {node_id: [list of GPU IDs']}
        Returns:
        list of GPU IDs on which to place the job
        boolean indicating if we found placement
        """
        # if there is a machine with exact required GPUs
        numGPUs_needed = job_param["num_GPUs"]
        for node in free_gpus:
            if len(free_gpus[node]) == numGPUs_needed:
                # found a perfect match
                return (free_gpus[node], True)
        # if we don't find an exact match find a node more GPUs
        # find the mode with min more GPUs then needed
        min_more_GPUs = 256  # random large enough number
        node_with_min_more_GPUs = None
        for node in free_gpus:
            if len(free_gpus[node]) >= numGPUs_needed:
                # found a node with more GPUs then needed
                if min_more_GPUs > len(free_gpus[node]):
                    min_more_GPUs = len(free_gpus[node])
                    node_with_min_more_GPUs = node
        if node_with_min_more_GPUs is not None:
            # only extracting the GPUs we need
            return (free_gpus[node_with_min_more_GPUs][:numGPUs_needed], True)
        # didn't find the requested number of GPUs
        return ([], False)

    def _scattered_placement(
        self, job_param: dict, free_gpus: dict
    ) -> Tuple[list, bool]:
        """
        Find placement without worrying about consolidation.
        Args:
        job_param: Job Param configuration
        free_gpus: Dict of free GPUs {node_id: [list of GPU IDs']}
        Returns:
        list of GPU IDs on which to place the job
        boolean indicating if we found placement
        """
        numGPUs_needed = job_param["num_GPUs"]
        gpus_for_job = list()
        found = False
        for node in free_gpus:
            gpus_for_job.extend(free_gpus[node][:numGPUs_needed])
            if len(gpus_for_job) == numGPUs_needed:
                found = True
                break
        if found:
            return (gpus_for_job, found)
        else:
            return ([], False)


# Gavel get job ids sorted by vals


def get_ids_sorted_by_priorities(priority_vals: dict) -> list:
    """
    Sorts the dict by value and return a sorted list in descending order of
    their priorities
    Args:
    priority_vals: key- job_id, vals- priority vals
    Returns:
    list of job ids sorted by their values
    """
    sorted_pairs = sorted(priority_vals.items(), key=lambda x: x[1], reverse=True)

    sorted_ids = [x for x, _ in sorted_pairs]
    return sorted_ids


# Pandas Utilities
def find_gpus_matching_JobID(job_id: int, gpu_df: pd.DataFrame) -> list:
    """
    Finds the GPU IDs which are running the given job id
    """
    return gpu_df.loc[gpu_df["JOB_IDS"] == job_id]["GPU_ID"].tolist()


# Find free GPUs


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


def find_free_GPUs_by_type(gpu_df: pd.DataFrame, gpu_type: str) -> dict:
    """
    Find free nodeID's which have free GPUs of specific type

    Args:
    gpu_df : DataFrame consiting the information about GPUs
    Returns:
    dict : {Node_ID : [list of free GPUs]}
    """
    return (
        gpu_df.loc[(gpu_df["IN_USE"] == False) & (gpu_df["GPU_type"] == gpu_type)]
        .groupby("Node_ID")["GPU_ID"]
        .apply(list)
        .to_dict()
    )


# Mark a GPU in use


def mark_gpu_in_use(gpu_df: pd.DataFrame, gpu_id: List[int], job_id: int) -> None:
    """
    Find the GPU ID and mark it in use. After deciding to schedule something on
    it.
    Args:
    gpu_df : DataFrame consisting of information about GPUs
    gpu_id : GPU to mark busy
    job_id: Job being scheduled on GPU with id=gpu_id

    Returns:
    None
    In place modifies the gpu_df
    """
    gpu_df.loc[gpu_df["GPU_ID"].isin(gpu_id), ["JOB_IDS", "IN_USE"]] = job_id, True
    return None


# Delete Job from data frame


def delete_job_by_id(gpu_df: pd.DataFrame, job_id: int) -> None:
    """
    Finds the job ID provided. Marks those jobs free and marks the GPU free to
    Args:
    gpu_df : DataFrame consisting of information about GPUs
    job_id : Job to delete

    Returns:
    None
    In place modifies the gpu_df
    """
    gpu_df.loc[gpu_df["JOB_IDS"] == job_id, ["JOB_IDS", "IN_USE"]] = None, False
    return None
