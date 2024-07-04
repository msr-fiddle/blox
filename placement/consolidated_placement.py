import pandas as pd
import copy
from typing import Tuple, List
from .placement_policy import JobPlacement
import utils


class ConsolidatedPlacement(JobPlacement):
    """
    Implements consolidated scheduling policy
    """

    def __init__(self, args):
        pass

    # the generator copies the arguments
    @JobPlacement.copy_arguments
    def place(
        self,
        active_jobs: dict,
        new_job_schedule: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        **kwargs
    ) -> dict:
        """
        Performing consolidated placement
        """

        running_jobs = 0
        new_scheduled_jobs = 0
        jobs_to_terminate = list()
        job_to_launch = dict()
        launched_job_ids = list()
        for idx, job_id in enumerate(job_order):
            job_id, _ = job_id
            job = active_jobs[job_id]
            found = False
            if job["is_running"] == True:
                # move to lower priority jobs
                running_jobs += 1
                continue
            if job["is_running"] == False:
                # need to find placement since the job is not running

                free_gpus = utils.find_free_gpus(gpu_df)
                placement, found = self._consolidated_placement(job, free_gpus)
                if not found:
                    # no free GPUs, need to remove GPUs for the training
                    for rev_idx in range(1, len(active_jobs) - idx):
                        potential_job_to_terminate = active_jobs[job_order[-rev_idx][0]]
                        if potential_job_to_terminate["is_running"] == True:
                            # terminate this job
                            jobs_to_terminate.append(job_order[-rev_idx][0])
                            potential_job_to_terminate["is_running"] = False
                            # freeing up GPUs
                            utils.delete_job_by_id(gpu_df, job_order[-rev_idx][0])
                            free_gpus = utils.find_free_GPUs(gpu_df)
                            placement, found = self._consolidated_placement(
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
                utils.mark_gpu_in_use(gpu_df, placement, job_id)
            else:
                # No point going down this path
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
