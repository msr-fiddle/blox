import time
import copy
import json
import pandas as pd
from blox_manager import BloxManager
from cluster_state import ClusterState
from job_state import JobState

from typing import Tuple, List


def remove_post_termination(jobs_terminated, job_state, cluster_state):
    for jid in jobs_terminated:
        _free_gpu_by_jobid(jid, cluster_state.gpu_df)
        # log the finished jobs
        job_state.finished_job[jid] = 1
        job_state.active_jobs.pop(jid)


def prune_jobs_based_on_runtime(
    job_state: JobState, cluster_state: ClusterState, blr: BloxManager
):
    """
    Special function. In regular cases jobs themselves exit based on some exit condition.
    The scheduler is not the one to decide when to terminate a job.
    However, for research we sometime will like to terminate jobs by the scheduler.

    There should be a key which says number_of_iteration to end
    """
    jid_to_terminate = list()

    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"] == True:
            if jid in job_state.active_jobs:
                if "tracked_metrics" in job_state.active_jobs[jid]:
                    if (
                        job_state.active_jobs[jid]["tracked_metrics"]["per_iter_time"]
                        > 0
                    ):

                        num_iterations = (
                            job_state.active_jobs[jid]["tracked_metrics"][
                                "attained_service"
                            ]
                            / job_state.active_jobs[jid]["tracked_metrics"][
                                "per_iter_time"
                            ]
                        )
                    else:
                        num_iterations = 0
                    if "num_total_iterations" in job_state.active_jobs[jid]:
                        if (
                            num_iterations
                            > job_state.active_jobs[jid]["num_total_iterations"]
                        ):
                            # TODO: put a condition to check if need
                            # plotting
                            if (
                                jid >= job_state.job_ids_to_track[0]
                                and jid <= job_state.job_ids_to_track[-1]
                            ):
                                # log the exit
                                if job_state.active_jobs[jid]["simulation"]:
                                    # if job is simulation
                                    job_state.job_completion_stats[jid] = [
                                        job_state.active_jobs[jid]["submit_time"],
                                        blr.simulator_time,
                                    ]
                                else:
                                    # if job is cluster job
                                    job_state.job_completion_stats[jid] = [
                                        job_state.active_jobs[jid]["submit_time"],
                                        time.time(),
                                    ]

                                job_state.job_runtime_stats[jid] = copy.deepcopy(
                                    job_state.active_jobs[jid]
                                )

                            jid_to_terminate.append(jid)
                            # delete GPU utilization
    return jid_to_terminate


def prune_jobs_based_on_runtime(
    job_state: JobState, cluster_state: ClusterState, blr: BloxManager
):
    """
    Special function. In regular cases jobs themselves exit based on some exit condition.
    The scheduler is not the one to decide when to terminate a job.
    However, for research we sometime will like to terminate jobs by the scheduler.

    There should be a key which says number_of_iteration to end
    """
    jid_to_terminate = list()

    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"] == True:
            if jid in job_state.active_jobs:
                if "tracked_metrics" in job_state.active_jobs[jid]:
                    if (
                        job_state.active_jobs[jid]["tracked_metrics"]["per_iter_time"]
                        > 0
                    ):

                        num_iterations = (
                            job_state.active_jobs[jid]["tracked_metrics"][
                                "attained_service"
                            ]
                            / job_state.active_jobs[jid]["tracked_metrics"][
                                "per_iter_time"
                            ]
                        )
                    else:
                        num_iterations = 0
                    if "num_total_iterations" in job_state.active_jobs[jid]:
                        if (
                            num_iterations
                            > job_state.active_jobs[jid]["num_total_iterations"]
                        ):
                            # TODO: put a condition to check if need
                            # plotting
                            if (
                                jid >= job_state.job_ids_to_track[0]
                                and jid <= job_state.job_ids_to_track[-1]
                            ):
                                # log the exit
                                if job_state.active_jobs[jid]["simulation"]:
                                    job_state.job_completion_stats[jid] = [
                                        job_state.active_jobs[jid]["submit_time"],
                                        blr.time,
                                    ]
                                else:
                                    # if job is cluster job
                                    job_state.job_completion_stats[jid] = [
                                        job_state.active_jobs[jid]["submit_time"],
                                        time.time(),
                                    ]

                                job_state.job_runtime_stats[jid] = copy.deepcopy(
                                    job_state.active_jobs[jid]
                                )

                            jid_to_terminate.append(jid)
                            # delete GPU utilization
    return jid_to_terminate


def prune_jobs_based_on_iteration(
    job_state: JobState, cluster_state: ClusterState, blr: BloxManager
):
    """
    Special function. In regular cases jobs themselves exit based on some exit condition.
    The scheduler is not the one to decide when to terminate a job.
    However, for research we sometime will like to terminate jobs by the scheduler.

    There should be a key which says number_of_iteration to end
    """
    jid_to_terminate = list()

    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"] == True:
            if jid in job_state.active_jobs:
                if "tracked_metrics" in job_state.active_jobs[jid]:
                    if "iter_num" in job_state.active_jobs[jid]["tracked_metrics"]:
                        num_iterations = job_state.active_jobs[jid]["tracked_metrics"][
                            "iter_num"
                        ]
                        if (
                            num_iterations
                            >= job_state.active_jobs[jid]["job_total_iteration"]
                        ):
                            if (
                                jid >= job_state.job_ids_to_track[0]
                                and jid <= job_state.job_ids_to_track[-1]
                            ):
                                # log the exit
                                job_state.job_completion_stats[jid] = [
                                    job_state.active_jobs[jid]["submit_time"],
                                    blr.time,
                                ]

                                job_state.job_runtime_stats[jid] = copy.deepcopy(
                                    job_state.active_jobs[jid]
                                )
                                # write logs everytime job is finished
                                write_log_files(job_state, cluster_state, blr)

                            jid_to_terminate.append(jid)
                        # delete GPU utilization
    return jid_to_terminate


def remove_post_termination(jobs_terminated, job_state, cluster_state):
    for jid in jobs_terminated:
        _free_gpu_by_jobid(jid, cluster_state.gpu_df)
        # log the finished jobs
        job_state.finished_job[jid] = 1
        job_state.active_jobs.pop(jid)


def prune_jobs(job_state: JobState, cluster_state: ClusterState, blr: BloxManager):
    """
    Delete jobs which have exited.
    """
    # terminate jobs based on seeing a job_exit key
    jid_to_terminate = list()
    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"] == True:
            if jid in job_state.active_jobs:
                if "tracked_metrics" in job_state.active_jobs[jid]:
                    if "job_exit" in job_state.active_jobs[jid]["tracked_metrics"]:
                        if (
                            job_state.active_jobs.get(jid)
                            .get("tracked_metrics")
                            .get("job_exit")
                            == True
                        ):
                            # TODO: put a condition to check if need
                            # plotting
                            if (
                                jid >= job_state.job_ids_to_track[0]
                                and jid <= job_state.job_ids_to_track[-1]
                            ):
                                # log the exit
                                job_state.job_completion_stats[jid] = [
                                    job_state.active_jobs[jid]["submit_time"],
                                    blr.time,
                                ]

                                job_state.job_runtime_stats[jid] = copy.deepcopy(
                                    job_state.active_jobs[jid]
                                )

                            jid_to_terminate.append(jid)
                            # delete GPU utilization
                            _free_gpu_by_jobid(jid, cluster_state.gpu_df)
                            # log the finished jobs
                            job_state.finished_job[jid] = 1

    # additional information for logging responsiveness
    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"] == True:
            if jid in job_state.active_jobs:
                if "job_launched_first_time" in job_state.active_jobs[jid]:
                    if (
                        job_state.active_jobs.get(jid).get("job_launched_first_time")
                        == True
                    ):
                        # TODO: put a condition to check if need
                        # plotting
                        if (
                            jid >= job_state.job_ids_to_track[0]
                            and jid <= job_state.job_ids_to_track[-1]
                        ):
                            # log the exit
                            job_state.job_responsiveness_stats[jid] = [
                                job_state.active_jobs[jid]["submit_time"],
                                blr.time,
                            ]

    for jid in jid_to_terminate:
        job_state.active_jobs.pop(jid)
    return None


def collect_custom_metrics(job_state, cluster_state, custom_metrics):
    """
    Collect any custom metrics
    """
    job_state.custom_metrics[job_state.time] = custom_metrics

    return None


def collect_cluster_job_metrics(
    job_state: JobState, cluster_state: ClusterState
) -> None:
    """
    Collect job state
    """
    total_jobs, jobs_in_queue, jobs_running = _get_jobs_status(job_state)

    # gpu utilization

    free_gpus = len(
        cluster_state.gpu_df[cluster_state.gpu_df["IN_USE"] == False]["GPU_ID"].tolist()
    )

    gpu_demand = 0
    for jid in job_state.active_jobs:
        gpu_demand += job_state.active_jobs[jid]["num_GPUs"]

    cluster_state.cluster_stats[cluster_state.time] = {
        "total_jobs": total_jobs,
        "jobs_in_queue": jobs_in_queue,
        "jobs_running": jobs_running,
        "free_gpus": free_gpus,
        "gpu_demand": gpu_demand,
    }


def track_finished_jobs(
    job_state: JobState, cluster_state: ClusterState, blr: BloxManager
) -> bool:
    """
    Once the jobs are finished exit from the trainer
    """
    # print("Finished jobs {}".format(soed([jid for jid in job_state.finished_job])))
    print(
        "Not finished job {}".format(
            list(set(job_state.job_ids_to_track) - set(job_state.finished_job))
        )
    )
    if all(jid in job_state.finished_job for jid in job_state.job_ids_to_track):
        write_log_files(job_state, cluster_state, blr)

        blr.terminate = True
        return True
    else:
        return False


def write_log_files(job_state, cluster_state, blr):

    with open(
        f"{blr.exp_prefix}_{job_state.job_ids_to_track[0]}_{job_state.job_ids_to_track[-1]}_{blr.scheduler_name}_load_{blr.load}_job_stats.json",
        "w",
    ) as fopen:
        # fopen.write(json.dumps(self.job_completion_stats))
        json.dump(job_state.job_completion_stats, fopen)

    with open(
        f"{blr.exp_prefix}_{job_state.job_ids_to_track[0]}_{job_state.job_ids_to_track[-1]}_{blr.scheduler_name}_load_{blr.load}_cluster_stats.json",
        "w",
    ) as fopen:
        # fopen.write(json.dumps(self.cluster_stats))
        json.dump(cluster_state.cluster_stats, fopen)
    # sys.exit(0)
    with open(
        f"{blr.exp_prefix}_{job_state.job_ids_to_track[0]}_{job_state.job_ids_to_track[-1]}_{blr.scheduler_name}_load_{blr.load}_run_time_stats.json",
        "w",
    ) as fopen:
        # fopen.write(json.dumps(self.cluster_stats))
        json.dump(job_state.job_runtime_stats, fopen)

    with open(
        f"{blr.exp_prefix}_{job_state.job_ids_to_track[0]}_{job_state.job_ids_to_track[-1]}_{blr.scheduler_name}_load_{blr.load}_responsivness.json",
        "w",
    ) as fopen:
        # fopen.write(json.dumps(self.cluster_stats))
        json.dump(job_state.job_responsiveness_stats, fopen)
    with open(
        f"{blr.exp_prefix}_{job_state.job_ids_to_track[0]}_{job_state.job_ids_to_track[-1]}_{blr.scheduler_name}_load_{blr.load}_custom_metrics.json",
        "w",
    ) as fopen:
        # fopen.write(json.dumps(self.cluster_stats))
        json.dump(job_state.custom_metrics, fopen)


def _get_jobs_status(job_state: JobState) -> Tuple[int]:
    """
    Get number of jobs running, jobs in queue and total jobs
    """
    total_jobs = len(job_state.active_jobs.keys())
    jobs_in_queue = 0
    jobs_running = 0
    for jid in job_state.active_jobs:
        if job_state.active_jobs[jid]["is_running"]:
            jobs_running += 1
        if not job_state.active_jobs[jid]["is_running"]:
            jobs_in_queue += 1
    return (total_jobs, jobs_in_queue, jobs_running)


# NOTE: Utilities for querying the GPU DF
def _find_ipaddr_by_job_ids(job_id: str, gpu_df: pd.DataFrame) -> List[str]:
    """
    Given a jobID finds the ip-addresses on which the job runs.
    Args:
        job_id: ID of the job to find corresponding ipaddress
    Returns:
        List of IP addresses on which the job is running
    """
    return gpu_df[gpu_df["JOB_IDS"] == job_id]["IP_addr"].tolist()


def _find_ipaddr_by_gpu_ids(gpu_ids: List[int], gpu_df: pd.DataFrame) -> List[str]:
    """
    Return the IP address for given GPU IDs

    Args:
        gpu_ids: GPU ids to search
    Returns:
        List of IP addresses for corresponding gpu_ids
    """

    ipaddress_to_launch = list()
    for gid in gpu_ids:
        gid_ipaddr = gpu_df[gpu_df["GPU_ID"] == gid]["IP_addr"].tolist()
        assert len(gid_ipaddr) == 1, "Multiple IP addr for same GPU, something wrong"

        ipaddress_to_launch.extend(gid_ipaddr)
    return ipaddress_to_launch


def _free_gpu_by_jobid(job_id: int, gpu_df: pd.DataFrame) -> None:
    """
    Marks the corresponding GPU free for a given job ID
    Args:
        job_id: ID of the job to terminate
    """
    gpu_df.loc[gpu_df["JOB_IDS"] == job_id, ["JOB_IDS", "IN_USE"]] = (
        None,
        False,
    )
    return None


def _mark_gpu_in_use_by_gpu_id(
    gpu_id_list: List[int], job_id: int, gpu_df: pd.DataFrame
) -> None:
    """
    Marks the corresponding GPU in use for a given job ID
    Args:
        gpu_id_list : List of GPU ID's to terminate
        job_id: ID of the job to terminate
    """
    gpu_df.loc[gpu_df["GPU_ID"].isin(gpu_id_list), ["JOB_IDS", "IN_USE"]] = (
        job_id,
        True,
    )
    return None


def _find_local_gpu_id(global_gpu_ids: List[int], gpu_df: pd.DataFrame) -> List[int]:
    """
    Given a list of Global GPU ID's find the corresponding local GPU id's

    Args:
        global_gpu_ids: Global GPU ID's
    Returns:
        local_gpu_ids: Local GPU id's corresponding to that value
    """
    local_gpu_id = list()
    # TODO: Get rid of this for loop using .isin
    for gid in global_gpu_ids:
        lgid = gpu_df[gpu_df["GPU_ID"] == gid]["Local_GPU_ID"].tolist()

        assert (
            len(lgid) == 1
        ), "Multiple Local GPUs for same global GPU ID, something wrong"

        local_gpu_id.extend(lgid)

    return local_gpu_id


# print(metric_data)
